const DEFAULT_QUOTA = {
  perizia_scans_remaining: 4,
  image_scans_remaining: 0,
  assistant_messages_remaining: 0,
};

const PLAN_LABELS = {
  free: 'Accesso iniziale',
  starter: 'Credit Pack 8',
  solo: 'Solo',
  pro: 'Pro',
  studio: 'Studio',
  enterprise: 'Interno',
};

export const getFeatureAccess = (user) => {
  const featureAccess = user?.account?.feature_access || user?.feature_access || {};

  return {
    canUseAssistant: Boolean(featureAccess.can_use_assistant),
    canUseImageForensics: Boolean(featureAccess.can_use_image_forensics),
  };
};

export const getAccountState = (user) => {
  const planId = user?.account?.effective_plan || user?.plan || 'free';
  const effectiveQuota = user?.account?.effective_quota || user?.quota || {};
  const featureAccess = getFeatureAccess(user);
  const periziaCredits = user?.account?.perizia_credits || user?.perizia_credits || {};
  const subscription = user?.account?.subscription || user?.subscription_state || {};

  return {
    isMasterAdmin: Boolean(user?.is_master_admin),
    planId,
    planLabel: PLAN_LABELS[planId] || String(planId || 'free'),
    quota: {
      perizia_scans_remaining: effectiveQuota.perizia_scans_remaining ?? DEFAULT_QUOTA.perizia_scans_remaining,
      image_scans_remaining: effectiveQuota.image_scans_remaining ?? DEFAULT_QUOTA.image_scans_remaining,
      assistant_messages_remaining: effectiveQuota.assistant_messages_remaining ?? DEFAULT_QUOTA.assistant_messages_remaining,
    },
    periziaCredits: {
      monthlyRemaining: periziaCredits.monthly_remaining ?? 0,
      extraRemaining: periziaCredits.extra_remaining ?? effectiveQuota.perizia_scans_remaining ?? DEFAULT_QUOTA.perizia_scans_remaining,
      totalAvailable: periziaCredits.total_available ?? effectiveQuota.perizia_scans_remaining ?? DEFAULT_QUOTA.perizia_scans_remaining,
      monthlyPlanId: periziaCredits.monthly_plan_id ?? null,
      packExpiryEnforced: Boolean(periziaCredits.pack_expiry_enforced),
    },
    subscription: {
      status: subscription.status ?? null,
      stripeCustomerId: subscription.stripe_customer_id ?? null,
      stripeSubscriptionId: subscription.stripe_subscription_id ?? null,
      currentPlanId: subscription.current_plan_id ?? null,
      currentPeriodEnd: subscription.current_period_end ?? null,
      cancelAtPeriodEnd: Boolean(subscription.cancel_at_period_end),
      pendingChange: Boolean(subscription.pending_change),
      pendingPlanId: subscription.pending_plan_id ?? null,
      pendingEffectiveAt: subscription.pending_effective_at ?? null,
    },
    featureAccess,
  };
};
