// main is the entry point for the MCP server.
// mainはMCPサーバーのエントリポイントです。
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/exec"

	// This package is used to generate unique IDs for our sessions.
	// このパッケージはセッションの一意のIDを生成するために使用されます。
	"github.com/google/uuid"

	pb "github.com/IceMask/mcp-server/proto/v1" // <-- 确保这里是你自己的模块路径
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// mcpServer is our implementation of the MCPServiceServer interface.
// mcpServerはMCPServiceServerインターフェースの私たちの実装です。
type mcpServer struct {
	pb.UnimplementedMCPServiceServer
	// The map now stores the command object itself for better control.
	// マップはより良い制御のためにコマンドオブジェクト自体を保存するようになりました。
	activeSessions map[string]*exec.Cmd
}

// NewMCPServer creates a new instance of our server.
// NewMCPServerはサーバーの新しいインスタンスを作成します。
func NewMCPServer() *mcpServer {
	return &mcpServer{
		activeSessions: make(map[string]*exec.Cmd),
	}
}

// findFreePort asks the kernel for a free open port that is ready to use.
// findFreePortはカーネルに使用可能な空きポートを問い合わせます。
func findFreePort() (int, error) {
	// Listen on port 0, which tells the OS to give us a random free port.
	// ポート0でリッスンすると、OSがランダムな空きポートを割り当ててくれます。
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close() // Ensure the listener is closed to free the port. ポートを解放するためにリスナーを確実に閉じます。
	return l.Addr().(*net.TCPAddr).Port, nil
}

// StartSession launches a new Appium server instance.
// StartSessionは新しいAppiumサーバーインスタンスを起動します。
func (s *mcpServer) StartSession(ctx context.Context, req *pb.StartSessionRequest) (*pb.StartSessionResponse, error) {
	log.Printf("Received StartSession request for device %s with app %s", req.DeviceId, req.AppId)

	// 1. Find a free port for the new Appium instance.
	// 1. 新しいAppiumインスタンスのために空きポートを見つけます。
	port, err := findFreePort()
	if err != nil {
		log.Printf("Error finding a free port: %v", err)
		return nil, fmt.Errorf("could not find a free port: %w", err)
	}
	log.Printf("Found free port for Appium: %d", port)

	// 2. Create the Appium command.
	// 2. Appiumコマンドを作成します。
	// We use `appium -p <port>` to start Appium on the specific port.
	// `appium -p <port>` を使用して、特定のポートでAppiumを起動します。
	cmd := exec.Command("appium", "-p", fmt.Sprintf("%d", port))

	// Optional: Redirect Appium's logs to a file for debugging.
	// オプション：デバッグのためにAppiumのログをファイルにリダイレクトします。
	// logfile, _ := os.Create(fmt.Sprintf("./appium-log-%d.txt", port))
	// cmd.Stdout = logfile
	// cmd.Stderr = logfile

	// 3. Start the command asynchronously.
	// 3. コマンドを非同期で開始します。
	if err := cmd.Start(); err != nil {
		log.Printf("Error starting Appium command: %v", err)
		return nil, fmt.Errorf("failed to start appium: %w", err)
	}

	// 4. Generate a unique session ID and store the process.
	// 4. 一意のセッションIDを生成し、プロセスを保存します。
	sessionID := uuid.New().String()
	s.activeSessions[sessionID] = cmd
	log.Printf("Appium server started for session %s on port %d with PID %d", sessionID, port, cmd.Process.Pid)

	// 5. Return the new session ID to the client.
	// 5. 新しいセッションIDをクライアントに返します。
	return &pb.StartSessionResponse{SessionId: sessionID}, nil
}

// EndSession terminates a running Appium server instance.
// EndSessionは実行中のAppiumサーバーインスタンスを終了させます。
func (s *mcpServer) EndSession(ctx context.Context, req *pb.EndSessionRequest) (*pb.EndSessionResponse, error) {
	sessionID := req.SessionId
	log.Printf("Received EndSession request for session %s", sessionID)

	cmd, ok := s.activeSessions[sessionID]
	if !ok {
		log.Printf("Session ID %s not found", sessionID)
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}

	// Kill the process.
	// プロセスを強制終了します。
	if err := cmd.Process.Kill(); err != nil {
		log.Printf("Failed to kill process for session %s (PID: %d): %v", sessionID, cmd.Process.Pid, err)
		// We still try to clean up the map even if killing fails.
		// 強制終了に失敗しても、マップのクリーンアップを試みます。
	} else {
		log.Printf("Successfully killed process for session %s (PID: %d)", sessionID, cmd.Process.Pid)
	}

	// Remove the session from our tracking map.
	// トラッキングマップからセッションを削除します。
	delete(s.activeSessions, sessionID)

	return &pb.EndSessionResponse{Message: "Session ended successfully"}, nil
}

// TapElement is the implementation for the TapElement RPC.
// TapElementはTapElement RPCの実装です。
func (s *mcpServer) TapElement(ctx context.Context, req *pb.TapElementRequest) (*pb.TapElementResponse, error) {
	log.Printf("Received TapElement request for session %s: description='%s'", req.SessionId, req.ElementDescription)
	return &pb.TapElementResponse{Message: "TapElement request processed successfully"}, nil
}

func main() {
	const port = ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	pb.RegisterMCPServiceServer(s, NewMCPServer())

	reflection.Register(s)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
