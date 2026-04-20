import grpc
from concurrent import futures
import time
import requests


from payment_pb2 import PaymentResponse
import payment_pb2_grpc
from main import create_payment  # используем существующий REST код для логики

class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    def CreatePayment(self, request, context):
        result = create_payment(amount=request.amount, idempotency_key=request.idempotency_key)
        return PaymentResponse(
            id=result['id'],
            status=result['status'],
            idempotency_key=result['idempotency_key']
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server running on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

 def send_webhook(payload):
            try:
                requests.post("http://localhost:8001/webhook", json=payload)
            except Exception as e:
                print("Webhook failed:", e)
if __name__ == "__main__":
    serve()
