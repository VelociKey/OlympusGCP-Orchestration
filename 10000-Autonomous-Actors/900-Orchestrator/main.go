package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/api/option"

	whisper "Olympus2/90000-Enablement-Labs/P0000-pkg/000-whisper"
	orchestrationv1 "OlympusGCP-Orchestration/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/orchestration/v1"
	"OlympusGCP-Orchestration/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/orchestration/v1/orchestrationv1connect"
)

type OrchestrationServer struct {
	pubsubClient *pubsub.Client
}

func (s *OrchestrationServer) Publish(ctx context.Context, req *connect.Request[orchestrationv1.PublishRequest]) (*connect.Response[orchestrationv1.PublishResponse], error) {
	t := s.pubsubClient.Topic(req.Msg.Topic)
	res := t.Publish(ctx, &pubsub.Message{Data: req.Msg.Data})
	id, _ := res.Get(ctx)
	return connect.NewResponse(&orchestrationv1.PublishResponse{MessageId: id}), nil
}

func (s *OrchestrationServer) CreateTask(ctx context.Context, req *connect.Request[orchestrationv1.TaskRequest]) (*connect.Response[orchestrationv1.TaskResponse], error) {
	taskID := fmt.Sprintf("task-%d", time.Now().Unix())
	slog.Info("Orchestration: Task Created", "id", taskID, "queue", req.Msg.Queue)
	return connect.NewResponse(&orchestrationv1.TaskResponse{TaskId: taskID}), nil
}

func (s *OrchestrationServer) CreateJob(ctx context.Context, req *connect.Request[orchestrationv1.JobRequest]) (*connect.Response[orchestrationv1.JobResponse], error) {
	jobID := fmt.Sprintf("job-%s", req.Msg.Name)
	return connect.NewResponse(&orchestrationv1.JobResponse{JobId: jobID}), nil
}

func (s *OrchestrationServer) ExecuteWorkflow(ctx context.Context, req *connect.Request[orchestrationv1.WorkflowRequest]) (*connect.Response[orchestrationv1.WorkflowResponse], error) {
	return connect.NewResponse(&orchestrationv1.WorkflowResponse{State: "SUCCEEDED", OutputJson: "{}"}), nil
}

func main() {
	slog.Info("OrchestrationManager: Booting Event-Driven Substrate...")
	w := whisper.New("OrchestrationManager", "gcp_orchestration.lpsv")
	defer w.Close()

	ctx := context.Background()
	psHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	if psHost == "" { psHost = "localhost:8085" }
	psClient, _ := pubsub.NewClient(ctx, "olympus-project", option.WithEndpoint(psHost), option.WithoutAuthentication())

	server := &OrchestrationServer{pubsubClient: psClient}
	mux := http.NewServeMux()
	mux.Handle(orchestrationv1connect.NewOrchestrationServiceHandler(server))

	port := "8090"
	slog.Info("OrchestrationManager: Listening...", "addr", "localhost:"+port)
	http.ListenAndServe("localhost:"+port, h2c.NewHandler(mux, &http2.Server{}))
}
