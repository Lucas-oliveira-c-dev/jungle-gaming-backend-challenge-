/**
 * Stable, machine-readable reasons for REJECTED / FAILED transactions.
 * `retryable` tells the provider whether resending the *same* payload can ever succeed.
 */
export enum FailureCode {
  // Balance
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalInsufficientFunds = 'REVERSAL_INSUFFICIENT_FUNDS',
  // Wallet / ownership
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  PlayerWalletMismatch = 'PLAYER_WALLET_MISMATCH',
  // References
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  ReferenceKindNotAllowed = 'REFERENCE_KIND_NOT_ALLOWED',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  ReversalAmountMismatch = 'REVERSAL_AMOUNT_MISMATCH',
  // Permanent infrastructure failure (status FAILED)
  ProcessingFailed = 'PROCESSING_FAILED',
}

export const FAILURE_CODE_DESCRIPTIONS: Record<FailureCode, { description: string; retryable: boolean }> = {
  [FailureCode.InsufficientFunds]: { description: 'BET larger than the available balance', retryable: false },
  [FailureCode.ReversalInsufficientFunds]: {
    description: 'Reversal (ROLLBACK of a credit) would make the balance negative',
    retryable: false,
  },
  [FailureCode.CurrencyMismatch]: { description: 'Operation currency differs from the wallet currency', retryable: false },
  [FailureCode.PlayerWalletMismatch]: { description: 'Wallet does not belong to the given player', retryable: false },
  [FailureCode.ReferenceNotFound]: {
    description: 'Referenced transaction never arrived within the retry window',
    retryable: false,
  },
  [FailureCode.ReferenceMismatch]: {
    description: 'Reference belongs to another player, wallet, currency or round',
    retryable: false,
  },
  [FailureCode.ReferenceKindNotAllowed]: {
    description: 'Reference kind cannot be targeted by this operation (e.g. REFUND of a WIN)',
    retryable: false,
  },
  [FailureCode.ReferenceNotProcessed]: { description: 'Referenced transaction was rejected or failed', retryable: false },
  [FailureCode.ReferenceAlreadyReversed]: { description: 'Referenced transaction was already reversed', retryable: false },
  [FailureCode.ReversalAmountMismatch]: {
    description: 'Reversal amount differs from the referenced amount (partial reversals are not supported)',
    retryable: false,
  },
  [FailureCode.ProcessingFailed]: { description: 'Permanent infrastructure failure, kept for audit', retryable: false },
};
