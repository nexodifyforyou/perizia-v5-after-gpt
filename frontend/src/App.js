import React, { useEffect } from "react";
import { BrowserRouter, Routes, Route, useLocation, Navigate } from "react-router-dom";
import { Toaster } from "./components/ui/sonner";
import { toast } from "sonner";

// Pages
import Landing from "./pages/Landing";
import Pacchetti from "./pages/Pacchetti";
import Supporto from "./pages/Supporto";
import Termini from "./pages/Termini";
import Privacy from "./pages/Privacy";
import Dashboard from "./pages/Dashboard";
import NewAnalysis from "./pages/NewAnalysis";
import AnalysisResult from "./pages/AnalysisResult";
import AnalysisPrintView from "./pages/AnalysisPrintView";
import ImageForensics from "./pages/ImageForensics";
import Assistant from "./pages/Assistant";
import History from "./pages/History";
import Billing from "./pages/Billing";
import Profile from "./pages/Profile";
import AuthCallback from "./pages/AuthCallback";
import AdminOverview from "./pages/admin/AdminOverview";
import AdminUsers from "./pages/admin/AdminUsers";
import AdminUserDetail from "./pages/admin/AdminUserDetail";
import AdminPerizie from "./pages/admin/AdminPerizie";
import AdminImages from "./pages/admin/AdminImages";
import AdminAssistant from "./pages/admin/AdminAssistant";
import AdminTransactions from "./pages/admin/AdminTransactions";
import AdminBetaFeedback from "./pages/admin/AdminBetaFeedback";
import BetaDashboard from "./pages/BetaDashboard";
import { Sidebar } from "./pages/Dashboard";

// Context
import { AuthProvider, useAuth } from "./context/AuthContext";

// Protected Route wrapper
const ProtectedRoute = ({ children }) => {
  const { user, loading } = useAuth();
  const location = useLocation();

  // NOTE: the `?debug=1` + window.__DEBUG_ANALYSIS_PAYLOAD__ auth bypass that
  // used to live here was removed with the legacy report body: it existed only
  // to let snapshot tooling render that body without a session. Nothing reads
  // that global on this route any more, and an auth bypass must not outlive
  // its purpose.

  if (loading) {
    return (
      <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-zinc-400 font-mono text-sm">Authenticating...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/" state={{ from: location }} replace />;
  }

  return children;
};

const AdminRoute = ({ children }) => {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-zinc-400 font-mono text-sm">Authenticating...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/" state={{ from: location }} replace />;
  }

  if (!user?.is_master_admin) {
    return <Navigate to="/dashboard" replace />;
  }

  return children;
};

const BetaRoute = ({ children }) => {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-zinc-400 font-mono text-sm">Authenticating...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/" state={{ from: location }} replace />;
  }

  if (!user?.is_beta_partner && !user?.is_master_admin) {
    return <Navigate to="/dashboard" replace />;
  }

  return children;
};

const FeatureUnavailablePage = ({ featureName }) => {
  const { user, logout } = useAuth();

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      <main className="px-4 pb-8 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
        <div className="max-w-2xl bg-zinc-900 border border-zinc-800 rounded-2xl p-8">
          <p className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-3">In arrivo</p>
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-3">{featureName}</h1>
          <p className="text-zinc-400 mb-2">Funzionalita non ancora disponibile.</p>
          <p className="text-sm text-zinc-500">Accesso non abilitato per questo account.</p>
        </div>
      </main>
    </div>
  );
};

const FeatureRoute = ({ children, canAccess, featureName }) => {
  const location = useLocation();

  useEffect(() => {
    if (!canAccess) {
      toast.error(`${featureName}: accesso non abilitato`);
    }
  }, [canAccess, featureName, location.pathname]);

  if (!canAccess) {
    return <FeatureUnavailablePage featureName={featureName} />;
  }

  return children;
};

// App Router Component
function AppRouter() {
  const location = useLocation();
  const { user, featureAccess } = useAuth();
  
  // REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
  // Check URL fragment OR query params for session_id (Emergent Auth callback)
  const hasSessionInHash = location.hash?.includes('session_id=');
  const isBillingCheckoutReturn =
    location.pathname === '/billing' &&
    location.search?.includes('checkout=') &&
    location.search?.includes('session_id=');
  const hasSessionInSearch = location.search?.includes('session_id=') && !isBillingCheckoutReturn;

  useEffect(() => {
    if (user?.is_master_admin) {
      const path = location.pathname + location.search;
      localStorage.setItem('last_path', path);
    }
  }, [location.pathname, location.search, user]);
  
  if (hasSessionInHash || hasSessionInSearch) {
    return <AuthCallback />;
  }

  return (
    <Routes>
      <Route path="/" element={<Landing />} />
      <Route path="/pacchetti" element={<Pacchetti />} />
      <Route path="/supporto" element={<Supporto />} />
      <Route path="/termini" element={<Termini />} />
      <Route path="/privacy" element={<Privacy />} />
      <Route path="/dashboard" element={
        <ProtectedRoute><Dashboard /></ProtectedRoute>
      } />
      <Route path="/analysis/new" element={
        <ProtectedRoute><NewAnalysis /></ProtectedRoute>
      } />
      {/* Storico lot workspace: the selected lot is URL-persisted as
          `?lot=<lot_id>` (read via useSearchParams inside AnalysisResult), so
          refresh / back / forward / deep links preserve it. The route itself
          stays `/analysis/:analysisId` — no lot path segment. */}
      <Route path="/analysis/:analysisId" element={
        <ProtectedRoute><AnalysisResult /></ProtectedRoute>
      } />
      {/* TEMPORARY print view: exists only for the backend headless PDF
          renderer (owner/admin sessions). Defence-in-depth guard here; the
          backend endpoint gating is the authority. Retired with the V2
          exporter. */}
      <Route path="/analysis/:analysisId/print" element={
        <AdminRoute><AnalysisPrintView /></AdminRoute>
      } />
      <Route path="/forensics" element={
        <ProtectedRoute>
          <FeatureRoute canAccess={featureAccess.canUseImageForensics} featureName="Image Forensics">
            <ImageForensics />
          </FeatureRoute>
        </ProtectedRoute>
      } />
      <Route path="/assistant" element={
        <ProtectedRoute>
          <FeatureRoute canAccess={featureAccess.canUseAssistant} featureName="Assistente">
            <Assistant />
          </FeatureRoute>
        </ProtectedRoute>
      } />
      <Route path="/history" element={
        <ProtectedRoute><History /></ProtectedRoute>
      } />
      <Route path="/billing" element={
        <ProtectedRoute><Billing /></ProtectedRoute>
      } />
      <Route path="/profile" element={
        <ProtectedRoute><Profile /></ProtectedRoute>
      } />
      <Route path="/beta/dashboard" element={
        <BetaRoute><BetaDashboard /></BetaRoute>
      } />
      <Route path="/admin" element={
        <AdminRoute><AdminOverview /></AdminRoute>
      } />
      <Route path="/admin/users" element={
        <AdminRoute><AdminUsers /></AdminRoute>
      } />
      <Route path="/admin/users/:user_id" element={
        <AdminRoute><AdminUserDetail /></AdminRoute>
      } />
      <Route path="/admin/perizie" element={
        <AdminRoute><AdminPerizie /></AdminRoute>
      } />
      <Route path="/admin/images" element={
        <AdminRoute><AdminImages /></AdminRoute>
      } />
      <Route path="/admin/assistant" element={
        <AdminRoute><AdminAssistant /></AdminRoute>
      } />
      <Route path="/admin/transactions" element={
        <AdminRoute><AdminTransactions /></AdminRoute>
      } />
      <Route path="/admin/beta-feedback" element={
        <AdminRoute><AdminBetaFeedback /></AdminRoute>
      } />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <div className="App min-h-screen bg-[#09090b]">
          <div className="noise-overlay"></div>
          <AppRouter />
          <Toaster 
            position="top-right"
            toastOptions={{
              style: {
                background: '#18181b',
                border: '1px solid #27272a',
                color: '#f4f4f5',
              },
            }}
          />
        </div>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;
