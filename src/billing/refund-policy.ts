export interface PaymentGateway {
  refund(paymentId: string): Promise<void>;
}

export async function refundPayment(
  gateway: PaymentGateway,
  paymentId: string,
): Promise<void> {
  await gateway.refund(paymentId);
  await gateway.refund(paymentId);
}
