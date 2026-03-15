const DEFAULT_QUOTA = {
  perizia_scans_remaining: 4,
  image_scans_remaining: 0,
  assistant_messages_remaining: 0,
};

const PLAN_LABELS = {
  free: 'Free',
  starter: 'Starter',
  solo: 'Solo',
  pro: 'Pro',
  studio: 'Studio',
  enterprise: 'Enterprise',
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

  return {
    isMasterAdmin: Boolean(user?.is_master_admin),
    planId,
    planLabel: PLAN_LABELS[planId] || String(planId || 'free'),
    quota: {
      perizia_scans_remaining: effectiveQuota.perizia_scans_remaining ?? DEFAULT_QUOTA.perizia_scans_remaining,
      image_scans_remaining: effectiveQuota.image_scans_remaining ?? DEFAULT_QUOTA.image_scans_remaining,
      assistant_messages_remaining: effectiveQuota.assistant_messages_remaining ?? DEFAULT_QUOTA.assistant_messages_remaining,
    },
    featureAccess,
  };
};
