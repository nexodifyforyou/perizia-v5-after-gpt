export const FEATURE_PREVIEW_ADMIN_EMAIL = 'nexodifyforyou@gmail.com';

export const getFeatureAccess = (user) => {
  const normalizedEmail = String(user?.email || '').trim().toLowerCase();
  const isPreviewAdmin = normalizedEmail === FEATURE_PREVIEW_ADMIN_EMAIL;

  return {
    isPreviewAdmin,
    canUseAssistant: isPreviewAdmin,
    canUseImageForensics: isPreviewAdmin,
  };
};
