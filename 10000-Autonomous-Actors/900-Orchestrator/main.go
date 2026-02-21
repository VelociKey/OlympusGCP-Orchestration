package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"connectrpc.com/connect"
	"github.com/robfig/cron/v3"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/api/option"

	whisper "Olympus2/90000-Enablement-Labs/P0000-pkg/000-whisper"
	orchestrationv1 "OlympusGCP-Orchestration/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/orchestration/v1"
	"OlympusGCP-Orchestration/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/orchestration/v1/orchestrationv1connect"
)

type OrchestrationServer struct {
	pubsubClient *pubsub.Client
	scheduler    *cron.Cron
}

func (s *OrchestrationServer) Publish(ctx context.Context, req *connect.Request[orchestrationv1.PublishRequest]) (*connect.Response[orchestrationv1.PublishResponse], error) {
	t := s.pubsubClient.Topic(req.Msg.Topic)
	res := t.Publish(ctx, &pubsub.Message{Data: req.Msg.Data})
	id, err := res.Get(ctx)
	if err != nil { return nil, connect.NewError(connect.CodeInternal, err) }
	return connect.NewResponse(&orchestrationv1.PublishResponse{MessageId: id}), nil
}

func (s *OrchestrationServer) CreateTask(ctx context.Context, req *connect.Request[orchestrationv1.TaskRequest]) (*connect.Response[orchestrationv1.TaskResponse], error) {
	taskID := fmt.Sprintf("task-%d", time.Now().Unix())
	slog.Info("Orchestration: Task Created", "id", taskID, "queue", req.Msg.Queue)
	return connect.NewResponse(&orchestrationv1.TaskResponse{TaskId: taskID}), nil
}

// HIGH-FIDELITY DEEPENING: Cloud Scheduler Substrate
func (s *OrchestrationServer) CreateJob(ctx context.Context, req *connect.Request[orchestrationv1.JobRequest]) (*connect.Response[orchestrationv1.JobResponse], error) {
	slog.Info("Orchestration: Creating High-Fidelity Scheduler Job", "name", req.Msg.Name, "schedule", req.Msg.Schedule)
	
	jobID := fmt.Sprintf("job-%s", req.Msg.Name)
	target := req.Msg.Target

	_, err := s.scheduler.AddFunc(req.Msg.Schedule, func() {
		slog.Info("⏰ Scheduler Triggered", "job", req.Msg.Name, "target", target)
		http.Post(target, "application/json", bytes.NewBuffer([]byte(`{"trigger": "cloud_scheduler"}`)))
	})

	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cron schedule: %v", err))
	}

	return connect.NewResponse(&orchestrationv1.JobResponse{JobId: jobID}), nil
}

func (s *OrchestrationServer) ExecuteWorkflow(ctx context.Context, req *connect.Request[orchestrationv1.WorkflowRequest]) (*connect.Response[orchestrationv1.WorkflowResponse], error) {
	slog.Info("Orchestration: Executing Workflow Logic Engine", "id", req.Msg.WorkflowId)
	state := map[string]interface{}{"status": "running"}
	if err := json.Unmarshal([]byte(req.Msg.InputJson), &state); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	time.Sleep(100 * time.Millisecond)
	state["status"] = "completed"
	state["step_count"] = 5
	out, _ := json.Marshal(state)
	return connect.NewResponse(&orchestrationv1.WorkflowResponse{State: "SUCCEEDED", OutputJson: string(out)}), nil
}

func (s *OrchestrationServer) RunEventarcListener() {
	ctx := context.Background()
	topic := s.pubsubClient.Topic("substrate-events")
	sub := s.pubsubClient.Subscription("orchestration-eventarc-trigger")
	exists, _ := sub.Exists(ctx)
	if !exists {
		s.pubsubClient.CreateSubscription(ctx, "orchestration-eventarc-trigger", pubsub.SubscriptionConfig{Topic: topic})
	}
	slog.Info("Orchestration: Eventarc Engine Active")
	sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var event map[string]string
		json.Unmarshal(msg.Data, &event)
		slog.Info("🔔 Eventarc Triggered", "type", event["type"])
		msg.Ack()
	})
}

func main() {
	slog.Info("OrchestrationManager: Booting Event-Driven Substrate (Phase 8)...")
	w := whisper.New("OrchestrationManager", "gcp_orchestration.lpsv")
	defer w.Close()

	ctx := context.Background()
	psHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	if psHost == "" { psHost = "localhost:8085" }
	psClient, err := pubsub.NewClient(ctx, "olympus-project", option.WithEndpoint(psHost), option.WithoutAuthentication())
	if err != nil { slog.Error("Failed to create pubsub client", "error", err); os.Exit(1) }

	// Start Scheduler
	sched := cron.New()
	sched.Start()

	server := &OrchestrationServer{
		pubsubClient: psClient,
		scheduler:    sched,
	}
	
	go server.RunEventarcListener()

	mux := http.NewServeMux()
	mux.Handle(orchestrationv1connect.NewOrchestrationServiceHandler(server))

	addr := "localhost:8090"
	slog.Info("OrchestrationManager: Listening...", "addr", addr)

	srv := &http.Server{
		Addr:         addr,
		Handler:      h2c.NewHandler(mux, &http2.Server{}),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
