import grpc
from payment_pb2 import PaymentRequest
import payment_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = payment_pb2_grpc.PaymentServiceStub(channel)
    response = stub.CreatePayment(PaymentRequest(amount=1500, idempotency_key="grpc-1"))
    print(f"Payment created: {response.id} | {response.status} | {response.idempotency_key}")

if __name__ == "__main__":
    run()
