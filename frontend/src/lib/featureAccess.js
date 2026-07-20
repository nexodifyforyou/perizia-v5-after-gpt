const DEFAULT_QUOTA = {
  perizia_scans_remaining: 4,
  image_scans_remaining: 0,
  assistant_messages_remaining: 0,
};

const PLAN_LABELS = {
  free: 'Accesso iniziale',
  starter: 'Pacchetto 8 crediti',
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

  // Beta program: an ACTIVE membership grants unlimited analyses (an entitlement,
  // never a wallet number). Drives the "Programma Beta" plan label and the
  // "Analisi illimitate" credit copy. Purchased credits remain preserved and are
  // still shown as "Crediti preservati" in Billing.
  const betaProgramRaw = user?.beta_program || {};
  const betaProgramActive = Boolean(betaProgramRaw.active ?? user?.is_beta_partner);
  // Beta quota: a second, orthogonal entitlement axis on top of membership
  // status (docs/beta_perizia_limits_plan.md §D/§O) — UNLIMITED (default,
  // matches today's unlimited-analyses behaviour) or LIMITED to N analyses per
  // phase. Absence of a `quota` block (older snapshot, or not yet rolled out)
  // is treated as UNLIMITED so existing beta testers keep today's experience.
  // Values are always taken verbatim from the API — never computed/derived
  // client-side, and never a placeholder like 9999.
  const quotaRaw = betaProgramRaw.quota || null;
  const quotaMode = quotaRaw?.mode || (betaProgramActive ? 'UNLIMITED' : null);
  const quotaState = quotaRaw?.state || (betaProgramActive ? 'UNLIMITED' : null);
  const betaProgram = {
    active: betaProgramActive,
    displayName: betaProgramRaw.display_name || user?.beta_partner_name || null,
    memberSince: betaProgramRaw.member_since || null,
    quota: {
      mode: quotaMode,
      state: quotaState,
      limit: quotaRaw?.limit ?? null,
      consumed: quotaRaw?.consumed ?? 0,
      reserved: quotaRaw?.reserved ?? 0,
      remaining: quotaRaw?.remaining ?? null,
      quotaVersion: quotaRaw?.quota_version ?? null,
    },
  };

  return {
    isMasterAdmin: Boolean(user?.is_master_admin),
    isBetaPartner: Boolean(user?.is_beta_partner),
    betaPartnerName: user?.beta_partner_name || null,
    betaPartnerType: user?.beta_partner_type || null,
    betaProgram,
    // Exact-owner flag that gates the Programma Beta admin surface on the client.
    // Backend authorization remains authoritative regardless of this flag.
    isExactOwner: Boolean(user?.correctness_v2_admin_view),
    planId,
    planLabel: betaProgramActive ? 'Programma Beta' : (PLAN_LABELS[planId] || String(planId || 'free')),
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
