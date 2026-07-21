import React, { createContext, useContext, useState, useEffect } from 'react';
import axios from 'axios';
import { getAccountState } from '../lib/featureAccess';

const AuthContext = createContext(null);

const API_URL = process.env.REACT_APP_BACKEND_URL;

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  // The backend is the authority on which login methods work. Default false so
  // the email option is hidden until the deployment says otherwise — a build
  // that ships ahead of the backend must not advertise a dead button. Google is
  // never gated on this: it is configured independently of email OTP.
  const [emailOtpEnabled, setEmailOtpEnabled] = useState(false);
  const accountState = getAccountState(user);

  useEffect(() => {
    checkAuth();
    loadAuthCapabilities();
  }, []);

  // Fetched per mount and never persisted, so flipping AUTH_EMAIL_ENABLED on
  // the backend takes effect on the next page load with no cache to bust.
  const loadAuthCapabilities = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/auth/capabilities`);
      setEmailOtpEnabled(response.data?.email_otp_enabled === true);
    } catch (error) {
      // Fail closed on the email option only; Google stays available.
      setEmailOtpEnabled(false);
    }
  };

  const checkAuth = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/auth/me`, {
        withCredentials: true
      });
      setUser(response.data);
    } catch (error) {
      setUser(null);
    } finally {
      setLoading(false);
    }
  };

  const login = () => {
    // REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
    const redirectUrl = window.location.origin + '/dashboard';
    window.location.href = `${API_URL}/api/auth/google/start?redirect=${encodeURIComponent(redirectUrl)}`;
  };

  // Passwordless email login. Provider-independent: the backend verifies
  // ownership of the address itself, so any mailbox works — Microsoft 365,
  // Aruba, a custom company domain, or Google.
  const requestEmailCode = async (email) => {
    const response = await axios.post(
      `${API_URL}/api/auth/email/request-code`,
      { email },
      { withCredentials: true }
    );
    return response.data;
  };

  const verifyEmailCode = async (challengeId, code) => {
    const response = await axios.post(
      `${API_URL}/api/auth/email/verify-code`,
      { challenge_id: challengeId, code },
      { withCredentials: true }
    );
    setUser(response.data.user);
    return response.data.user;
  };

  const exchangeSession = async (sessionId) => {
    try {
      const response = await axios.post(`${API_URL}/api/auth/session`, 
        { session_id: sessionId },
        { withCredentials: true }
      );
      setUser(response.data.user);
      return response.data.user;
    } catch (error) {
      console.error('Session exchange failed:', error);
      throw error;
    }
  };

  const logout = async () => {
    try {
      await axios.post(`${API_URL}/api/auth/logout`, {}, { withCredentials: true });
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      localStorage.removeItem('last_path');
      setUser(null);
    }
  };

  const refreshUser = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/auth/me`, {
        withCredentials: true
      });
      setUser(response.data);
      return response.data;
    } catch (error) {
      console.error('Refresh user failed:', error);
      return null;
    }
  };

  return (
    <AuthContext.Provider value={{ 
      user, 
      featureAccess: accountState.featureAccess,
      accountState,
      loading,
      emailOtpEnabled,
      login,
      logout,
      exchangeSession,
      requestEmailCode,
      verifyEmailCode,
      refreshUser,
      setUser 
    }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
