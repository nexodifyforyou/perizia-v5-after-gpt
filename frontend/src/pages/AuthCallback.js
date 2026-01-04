import React, { useEffect, useRef, useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

const AuthCallback = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { exchangeSession } = useAuth();
  const hasProcessed = useRef(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Prevent double execution in StrictMode
    if (hasProcessed.current) return;
    hasProcessed.current = true;

    const processAuth = async () => {
      try {
        // Extract session_id from URL fragment or query params
        const hash = window.location.hash;
        const search = window.location.search;
        
        let sessionId = null;
        
        // Try hash first
        const hashMatch = hash.match(/session_id=([^&]+)/);
        if (hashMatch) {
          sessionId = hashMatch[1];
        }
        
        // Try query params if not in hash
        if (!sessionId) {
          const params = new URLSearchParams(search);
          sessionId = params.get('session_id');
        }
        
        // Also check if session_id is in the pathname (some edge cases)
        if (!sessionId && location.pathname.includes('session_id')) {
          const pathMatch = location.pathname.match(/session_id=([^&/]+)/);
          if (pathMatch) {
            sessionId = pathMatch[1];
          }
        }
        
        if (!sessionId) {
          console.error('No session_id found in URL');
          console.log('Hash:', hash);
          console.log('Search:', search);
          console.log('Pathname:', location.pathname);
          setError('No session ID found. Please try logging in again.');
          setTimeout(() => navigate('/', { replace: true }), 2000);
          return;
        }

        console.log('Exchanging session_id:', sessionId.substring(0, 10) + '...');
        
        // Exchange session_id for session_token
        const user = await exchangeSession(sessionId);
        
        console.log('Session exchanged successfully, user:', user?.email);
        
        // Clear the hash and search from URL and navigate to dashboard
        window.history.replaceState(null, '', '/dashboard');
        navigate('/dashboard', { replace: true });
        
      } catch (error) {
        console.error('Auth callback error:', error);
        setError('Authentication failed. Please try again.');
        setTimeout(() => navigate('/', { replace: true }), 2000);
      }
    };

    processAuth();
  }, [exchangeSession, navigate, location]);

  return (
    <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
      <div className="text-center">
        {error ? (
          <>
            <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-6">
              <span className="text-3xl">⚠️</span>
            </div>
            <h2 className="text-2xl font-serif text-zinc-100 mb-2">Authentication Error</h2>
            <p className="text-red-400 font-mono text-sm">{error}</p>
            <p className="text-zinc-500 text-xs mt-2">Redirecting...</p>
          </>
        ) : (
          <>
            <div className="w-16 h-16 border-4 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-6"></div>
            <h2 className="text-2xl font-serif text-zinc-100 mb-2">Authenticating</h2>
            <p className="text-zinc-400 font-mono text-sm">Securing your session...</p>
          </>
        )}
      </div>
    </div>
  );
};

export default AuthCallback;
