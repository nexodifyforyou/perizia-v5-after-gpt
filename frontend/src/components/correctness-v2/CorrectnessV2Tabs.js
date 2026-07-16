import React, { useState } from 'react';
import { LayoutDashboard, User } from 'lucide-react';
import CustomerReportView from './CustomerReportView';
import CorrectnessV2Panel from './CorrectnessV2Panel';

// Role-aware container for the Correctness V2 surface.
//
//   canSeeAdminTab reveals the technical "Vista admin" tab (exact-email
//                  operator only). Everyone who reaches this surface sees the
//                  sanitized "Report cliente" tab; the admin tab is additive.
//
// This surface is the ONLY report surface: the parent (AnalysisResult) mounts
// it for every role once the availability probe resolves. CustomerReportView
// owns every customer sub-state internally, and this component never leaks
// admin/debug data into the customer tab.
//
// The Customer Report is served from the sanitized customer-view endpoint and
// never contains admin/debug/quality/artifact data. The Admin View is the full
// existing panel (run controls, status, quality, raw evidence).
const TABS = {
  customer: { label: 'Report cliente', icon: User },
  admin: { label: 'Vista admin', icon: LayoutDashboard },
};

const CorrectnessV2Tabs = ({ analysisId, canSeeAdminTab = false, customerState }) => {
  const [active, setActive] = useState('customer');

  const showAdminTab = Boolean(canSeeAdminTab);
  const activeTab = active === 'admin' && !showAdminTab ? 'customer' : active;
  const visibleTabs = showAdminTab ? ['customer', 'admin'] : ['customer'];

  return (
    <section className="mb-8 space-y-4 rounded-xl border border-gold/25 bg-gradient-to-b from-zinc-900/90 to-zinc-950/80 p-4 sm:p-5">
      <div className="flex flex-wrap items-center gap-2">
        {/* Internal mode name is admin-facing only; customers just see the report. */}
        {showAdminTab && (
          <p className="mr-2 text-[11px] font-mono uppercase text-gold">Correctness Mode V2</p>
        )}
        <div role="tablist" aria-label="Report" className="flex gap-1 rounded-lg border border-zinc-800 bg-zinc-950 p-1">
          {visibleTabs.map((key) => {
            const tab = TABS[key];
            const Icon = tab.icon;
            const isActive = key === activeTab;
            return (
              <button
                key={key}
                type="button"
                role="tab"
                aria-selected={isActive}
                data-testid={`cv2-tab-${key}`}
                onClick={() => setActive(key)}
                className={`inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-gold text-zinc-950'
                    : 'text-zinc-400 hover:text-zinc-100'
                }`}
              >
                <Icon className="h-4 w-4" />
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      {activeTab === 'admin' && showAdminTab ? (
        <div data-testid="cv2-admin-tab-panel">
          <CorrectnessV2Panel analysisId={analysisId} isAdmin />
        </div>
      ) : (
        <div data-testid="cv2-customer-tab-panel">
          <CustomerReportView analysisId={analysisId} state={customerState} />
        </div>
      )}
    </section>
  );
};

export default CorrectnessV2Tabs;
