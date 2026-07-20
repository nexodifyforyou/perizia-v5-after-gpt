import React, { useState } from 'react';
import AdminLayout from './AdminLayout';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../components/ui/tabs';
import OverviewTab from './betaProgram/OverviewTab';
import TestersTab from './betaProgram/TestersTab';
import FeedbackTab from './betaProgram/FeedbackTab';
import SignalsTab from './betaProgram/SignalsTab';

const TAB_IDS = ['panoramica', 'tester', 'feedback', 'segnali'];

// Initial tab may be deep-linked via `?tab=<id>` (e.g. the old
// /admin/beta-feedback redirect lands on `?tab=feedback`). Read directly off
// window.location rather than useSearchParams so this works identically
// whether or not a Router has re-mounted the page for the redirect. Unknown
// or missing values fall back to Panoramica.
const initialTabFromLocation = () => {
  if (typeof window === 'undefined') return 'panoramica';
  const requested = new URLSearchParams(window.location.search).get('tab');
  return TAB_IDS.includes(requested) ? requested : 'panoramica';
};

// Programma Beta — the single exact-owner admin surface for the beta program.
// Four tabs; one route (/admin/beta-program). Backend authorization is
// authoritative on every underlying API call.
const AdminBetaProgram = () => {
  const [tab, setTab] = useState(initialTabFromLocation);

  return (
    <AdminLayout title="Programma Beta" subtitle="Gestione tester, feedback e segnali operativi del programma beta">
      <Tabs value={tab} onValueChange={setTab} className="w-full">
        <TabsList className="mb-6 flex flex-wrap gap-1 bg-zinc-900 border border-zinc-800">
          <TabsTrigger value="panoramica" data-testid="beta-tab-panoramica">Panoramica</TabsTrigger>
          <TabsTrigger value="tester" data-testid="beta-tab-tester">Tester</TabsTrigger>
          <TabsTrigger value="feedback" data-testid="beta-tab-feedback">Feedback</TabsTrigger>
          <TabsTrigger value="segnali" data-testid="beta-tab-segnali">Segnali</TabsTrigger>
        </TabsList>

        <TabsContent value="panoramica"><OverviewTab active={tab === 'panoramica'} /></TabsContent>
        <TabsContent value="tester"><TestersTab active={tab === 'tester'} /></TabsContent>
        <TabsContent value="feedback"><FeedbackTab active={tab === 'feedback'} /></TabsContent>
        <TabsContent value="segnali"><SignalsTab active={tab === 'segnali'} /></TabsContent>
      </Tabs>
    </AdminLayout>
  );
};

export default AdminBetaProgram;
