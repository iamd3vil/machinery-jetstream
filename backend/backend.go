package backend

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/RichardKnop/machinery/v2/backends/iface"
	"github.com/RichardKnop/machinery/v2/common"
	mconfig "github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/nats-io/nats.go"
)

type Backend struct {
	common.Backend
	kv nats.KeyValue
}

func New(cnf *mconfig.Config, cfg Config) (iface.Backend, error) {
	opt := []nats.Option{}

	if cfg.EnabledAuth {
		opt = append(opt, nats.UserInfo(cfg.Username, cfg.Password))
	}

	conn, err := nats.Connect(cfg.URL, opt...)
	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue("machinery")
	if err != nil {
		return nil, err
	}

	return &Backend{
		Backend: common.NewBackend(cnf),
		kv:      kv,
	}, nil
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	if _, err := b.kv.Put(groupUUID, encoded); err != nil {
		return err
	}

	return nil
}

func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	// TODO: Do we need some kind of lock so that multiple workers don't trigger at the same time?
	// Not sure how to do this with jetstream kv yet.
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	_, err = b.kv.Put(groupUUID, encoded)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	item, err := b.kv.Get(taskUUID)
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item.Value()))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	return b.kv.Purge(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	return b.kv.Purge(groupUUID)
}

func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.kv.Get(groupUUID)
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item.Value()))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, 0, len(taskUUIDs))
	for _, uuid := range taskUUIDs {
		ts, err := b.kv.Get(uuid)
		if err != nil {
			return taskStates, err
		}

		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(ts.Value()))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			log.ERROR.Print(err)
			return taskStates, err
		}
		taskStates = append(taskStates, taskState)
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	if _, err := b.kv.Put(taskState.TaskUUID, encoded); err != nil {
		return err
	}

	return nil
}
