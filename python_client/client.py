# -*- coding: utf-8 -*-
"""
This script is a client that demonstrates how to call the MCP gRPC server.
In a real-world scenario, the functions in this script would be the "tools"
that a Large Language Model (LLM) would call.

このスクリプトは、MCP gRPCサーバーを呼び出す方法を示すクライアントです。
実際のシナリオでは、このスクリプト内の関数が、
大規模言語モデル（LLM）が呼び出す「ツール」になります。
"""
import grpc
# Import the generated Python files.
# 生成されたPythonファイルをインポートします。
from proto.v1 import mcp_pb2
from proto.v1 import mcp_pb2_grpc

def run_test():
    """
    Connects to the server and runs a start/end session test.
    サーバーに接続し、セッションの開始/終了テストを実行します。
    """
    # Establish a connection to the gRPC server.
    # gRPCサーバーへの接続を確立します。
    # Note: We are using an "insecure" channel because our server doesn't have TLS.
    # 注意：サーバーにTLSがないため、「insecure」チャネルを使用しています。
    with grpc.insecure_channel('localhost:50051') as channel:
        # Create a client stub.
        # クライアントスタブを作成します。
        stub = mcp_pb2_grpc.MCPServiceStub(channel)

        # --- This is what the LLM would do first ---
        # --- これはLLMが最初に行うことです ---
        print("--- Calling StartSession ---")
        # Log in English only.
        print("Log: Calling StartSession with device_id='emulator-5554'...")
        try:
            start_response = stub.StartSession(
                mcp_pb2.StartSessionRequest(
                    device_id='emulator-5554',
                    app_id='com.android.settings'
                )
            )
            session_id = start_response.session_id
            # Log in English only.
            print(f"Log: Successfully started session. Received session_id: {session_id}")
            print("--------------------------\n")

            # --- Then, the LLM would perform its tasks... ---
            # --- そして、LLMはタスクを実行します... ---
            # (For now, we just wait a bit)
            # (今は少し待つだけです)
            import time
            print("--- Pretending to do work for 5 seconds ---")
            time.sleep(5)
            print("-------------------------------------------\n")


            # --- Finally, the LLM would end the session ---
            # --- 最後に、LLMはセッションを終了します ---
            print("--- Calling EndSession ---")
            # Log in English only.
            print(f"Log: Calling EndSession with session_id: {session_id}...")
            end_response = stub.EndSession(
                mcp_pb2.EndSessionRequest(session_id=session_id)
            )
            # Log in English only.
            print(f"Log: Server responded with: '{end_response.message}'")
            print("------------------------\n")

        except grpc.RpcError as e:
            # Log in English only.
            print(f"An error occurred: {e.status()}: {e.details()}")

if __name__ == '__main__':
    run_test()