# Nexodify Forensic Engine - Product Requirements Document

## Original Problem Statement
Build a deterministic, audit-grade analyzer for Italian real-estate perizie/CTU documents + site photos + user Q&A. The system produces structured, repeatable JSON outputs with evidence tracking, risk assessment (semaforo system), and cost calculations.

## User Choices
1. **AI Integration**: Emergent LLM key (Gemini 2.5 Flash)
2. **Document Upload**: PDF-only (hardwired validation)
3. **Authentication**: Emergent-managed Google OAuth
4. **Payments**: Stripe integration with master admin bypass (admin@nexodify.com)
5. **Design**: Dark professional theme (forensic/legal feel)

## User Personas
1. **Italian Real Estate Investors**: Need quick risk assessment for auction properties
2. **Legal Professionals**: Require evidence-tracked analysis for due diligence
3. **Property Analysts**: Need comprehensive cost breakdowns and compliance checks

## Core Requirements (Static)
- PDF perizia document analysis with AI
- Semaforo (RED/AMBER/GREEN) risk assessment
- Money Box A-H cost calculations
- Legal Killers checklist (8 items)
- Evidence tracking with page numbers
- Bilingual output (Italian/English)
- Image forensics for site photos
- AI-powered Q&A assistant
- User authentication with Google OAuth
- Subscription plans (Free/Pro/Enterprise)
- Payment processing via Stripe
- Master admin bypass for payments

## What's Been Implemented (January 2026)

### Backend (FastAPI)
- [x] User authentication with Emergent Google OAuth
- [x] Session management with cookies
- [x] Subscription plans (Free/Pro/Enterprise)
- [x] Stripe payment integration
- [x] Master admin bypass (admin@nexodify.com gets Enterprise)
- [x] PDF perizia analysis with Gemini AI
- [x] Image forensics endpoint
- [x] AI assistant Q&A endpoint
- [x] User history endpoints
- [x] Dashboard statistics
- [x] Quota management

### Frontend (React)
- [x] Landing page with hero, features, pricing
- [x] Google OAuth login flow
- [x] Dashboard with stats and recent analyses
- [x] New Analysis page with PDF upload
- [x] Analysis Results page with full report
- [x] Image Forensics page
- [x] AI Assistant chat interface
- [x] History page with tabs
- [x] Billing/Subscription page
- [x] Profile page
- [x] Dark professional theme
- [x] Semaforo risk indicators
- [x] Money Box display

### Integrations
- [x] Emergent LLM Key (Gemini 2.5 Flash)
- [x] Emergent Google OAuth
- [x] Stripe Payments

## Prioritized Backlog

### P0 (Critical - Must Have)
- [x] Core perizia analysis
- [x] User authentication
- [x] Payment gating

### P1 (High Priority)
- [ ] PDF report generation/export
- [ ] Corrections/revision loop support
- [ ] Enhanced evidence extraction with anchors
- [ ] Real vision model for image forensics

### P2 (Medium Priority)
- [ ] Multi-document comparison
- [ ] Historical trend analysis
- [ ] Email notifications
- [ ] Admin dashboard

### P3 (Nice to Have)
- [ ] Mobile responsive improvements
- [ ] Dark/light theme toggle
- [ ] API access for Enterprise users
- [ ] Batch document processing

## Next Tasks List
1. Add PDF report export functionality
2. Implement corrections loop for revising analyses
3. Enhance perizia extraction with specific field detection
4. Add real vision model integration for image forensics
5. Implement email notifications for analysis completion
