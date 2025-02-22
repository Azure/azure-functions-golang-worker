package test

// func startWorkerServer(workerAddr string) {
// 	dispatcher := &internal.Dispatcher{}
// 	go func() {
// 		err := function.StartWorkerServer(workerAddr, dispatcher)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}()
// }

// func newFunctionRpcStream(ctx context.Context, workerAddr string) (*grpc.ClientConn, functionrpc.FunctionRpc_EventStreamClient, error) {
// 	conn, err := grpc.NewClient(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	client := functionrpc.NewFunctionRpcClient(conn)
// 	stream, err := client.EventStream(ctx)
// 	if err != nil {
// 		conn.Close()
// 		return nil, nil, err
// 	}

// 	return conn, stream, nil
// }

// func TestWorkerInvocationRequest(t *testing.T) {
// 	// Arrange
// 	testInvocationId := "invoke-123"
// 	workerAddr := "localhost:50053"
// 	startWorkerServer(workerAddr)

// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	clientConn, stream, err := newFunctionRpcStream(ctx, workerAddr)
// 	assert.NoError(t, err)
// 	defer clientConn.Close()

// 	// Act
// 	req := &functionrpc.StreamingMessage{
// 		RequestId: "test-invocation",
// 		Content: &functionrpc.StreamingMessage_InvocationRequest{
// 			InvocationRequest: &functionrpc.InvocationRequest{
// 				InvocationId: testInvocationId,
// 				FunctionId:   "func-abc",
// 			},
// 		},
// 	}

// 	assert.NoError(t, stream.Send(req))

// 	resp, err := stream.Recv()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, resp)

// 	// Assert
// 	if invocationResp, ok := resp.Content.(*functionrpc.StreamingMessage_InvocationResponse); ok {
// 		t.Logf("Invocation ID sent to worker: %s\n", invocationResp.InvocationResponse.InvocationId)
// 		assert.Equal(t, testInvocationId, invocationResp.InvocationResponse.InvocationId)
// 	} else {
// 		t.Fatalf("Unexpected response type: %+v", resp)
// 	}
// }
