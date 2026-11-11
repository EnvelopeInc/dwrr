package dwrr_test

import (
	"testing"

	"github.com/EnvelopeInc/dwrr"
	"github.com/stretchr/testify/require"
)

func TestPublicCostAwareAPI(t *testing.T) {
	scheduler := dwrr.NewDWRR[string](2)
	require.NoError(t, scheduler.SetQuantum(0, 2))
	require.NoError(t, scheduler.SetQuantum(1, 3))
	require.NoError(t, scheduler.Enqueue(0, []dwrr.Item[string]{{Value: "a", Cost: 2}, {Value: "b", Cost: 2}}))
	require.NoError(t, scheduler.Enqueue(1, []dwrr.Item[string]{{Value: "x", Cost: 4}, {Value: "y", Cost: 1}}))
	require.Equal(t, []dwrr.Scheduled[string]{{Queue: 0, Value: "a", Cost: 2}}, scheduler.Take(1))
	require.Equal(t, []dwrr.Scheduled[string]{{Queue: 0, Value: "b", Cost: 2}}, scheduler.Take(1))
	require.Equal(t, []dwrr.Scheduled[string]{{Queue: 1, Value: "x", Cost: 4}}, scheduler.Take(1))
	require.Equal(t, []dwrr.Scheduled[string]{{Queue: 1, Value: "y", Cost: 1}}, scheduler.Take(1))
}

func TestPublicDirectQueueRemovalAPI(t *testing.T) {
	scheduler := dwrr.NewDWRR[string](0)
	queue := scheduler.AddQueue()
	require.Equal(t, uint(0), queue)
	require.NoError(t, scheduler.Enqueue(queue, []dwrr.Item[string]{{Value: "head", Cost: 1}, {Value: "tail", Cost: 2}}))
	item, ok := scheduler.RemoveHead(queue)
	require.True(t, ok)
	require.Equal(t, dwrr.Item[string]{Value: "head", Cost: 1}, item)
	require.Equal(t, []dwrr.Item[string]{{Value: "tail", Cost: 2}}, scheduler.DrainQueue(queue))
	item, ok = scheduler.RemoveHead(queue)
	require.False(t, ok)
	require.Equal(t, dwrr.Item[string]{}, item)
}
