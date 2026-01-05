#!/usr/bin/env python3
"""
CRITICAL TEST: Nexodify Forensic Engine - IMPROVED PDF Extraction Test
Testing the specific perizia document with expected data validation
"""

import requests
import json
import sys
import os
from datetime import datetime
import subprocess

class PeriziaExtractionTester:
    def __init__(self, base_url="https://repo-setup-31.preview.emergentagent.com"):
        self.base_url = base_url
        self.token = None
        self.test_results = []
        self.critical_failures = []
        
        # Expected data from the reference document
        self.expected_data = {
            "header": {
                "procedure": "R.G.E. 62/2024",
                "tribunale": "TRIBUNALE DI MANTOVA",
                "address": "Via Sordello n. 5, San Giorgio Bigarello (MN)",
                "lotto": "Lotto Unico"
            },
            "prices": {
                "prezzo_base_asta": 391849.00,  # EUR 391.849,00 (page 45)
                "valore_stima": 419849.00,      # EUR 419.849,00 (page 40)
                "deprezzamenti": {
                    "regolarizzazioni": 23000,   # EUR 23.000 (page 40)
                    "vizi_occulti": 5000         # EUR 5.000 (page 40)
                }
            },
            "money_box": {
                "A": {"name": "Regolarizzazione urbanistica", "amount": 23000, "source": "Perizia p. 40"},
                "B": {"name": "Completamento finiture/impianti", "amount": 15000, "source": "Perizia p. 35"},
                "C": {"name": "Ottenimento abitabilità", "amount": 5000, "source": "Perizia p. 35"},
                "D": {"name": "Spese condominiali", "amount": None, "source": "Non specificato"},
                "E": {"name": "Cancellazione formalità", "amount": None, "source": "Non specificato"},
                "F": {"name": "Costo liberazione", "amount": 1500, "source": "prudenziale"}
            },
            "conformita": {
                "urbanistica": "regolare ai sensi L. 47/85",
                "condono": {"pratica": "1991", "definita": "1994"},
                "agibilita": "NON risulta",
                "impianti": "assenza conformità elettrico/termico/idrico",
                "ape": "assente"
            },
            "occupazione": "Occupato dal debitore esecutato",
            "legal_killers": {
                "usi_civici": "non risultano da CDU (p. 44)"
            },
            "checklist_items": 5,
            "qa_checks": 9
        }

    def create_test_user_and_session(self):
        """Create test user with master admin privileges"""
        print("🔧 Creating test user with master admin privileges...")
        
        mongo_script = f"""
        use('test_database');
        var userId = 'test-master-' + Date.now();
        var sessionToken = 'test_session_' + Date.now();
        db.users.insertOne({{
          user_id: userId,
          email: 'nexodifyforyou@gmail.com',
          name: 'Test Master Admin',
          picture: 'https://via.placeholder.com/150',
          plan: 'enterprise',
          is_master_admin: true,
          quota: {{
            perizia_scans_remaining: 9999,
            image_scans_remaining: 9999,
            assistant_messages_remaining: 9999
          }},
          created_at: new Date()
        }});
        db.user_sessions.insertOne({{
          session_id: 'sess_' + Date.now(),
          user_id: userId,
          session_token: sessionToken,
          expires_at: new Date(Date.now() + 7*24*60*60*1000),
          created_at: new Date()
        }});
        print('SESSION_TOKEN:' + sessionToken);
        print('USER_ID:' + userId);
        """
        
        try:
            result = subprocess.run(['mongosh', '--eval', mongo_script], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                output = result.stdout
                session_token = None
                user_id = None
                
                for line in output.split('\n'):
                    if line.startswith('SESSION_TOKEN:'):
                        session_token = line.split(':', 1)[1]
                    elif line.startswith('USER_ID:'):
                        user_id = line.split(':', 1)[1]
                
                if session_token and user_id:
                    print(f"✅ Created master admin user: {user_id}")
                    print(f"✅ Created session token: {session_token}")
                    self.token = session_token
                    return session_token, user_id
                else:
                    print("❌ Failed to extract session token or user ID")
                    return None, None
            else:
                print(f"❌ MongoDB script failed: {result.stderr}")
                return None, None
                
        except Exception as e:
            print(f"❌ Error creating test user: {e}")
            return None, None

    def upload_perizia_document(self):
        """Upload the specific perizia document for analysis"""
        if not self.token:
            print("❌ No authentication token available")
            return None, None
        
        pdf_path = "/app/perizia_test.pdf"
        if not os.path.exists(pdf_path):
            print(f"❌ PDF file not found: {pdf_path}")
            return None, None
        
        print(f"📄 Uploading perizia document: {pdf_path}")
        
        url = f"{self.base_url}/api/analysis/perizia"
        headers = {'Authorization': f'Bearer {self.token}'}
        
        try:
            with open(pdf_path, 'rb') as f:
                files = {'file': ('perizia_test.pdf', f, 'application/pdf')}
                response = requests.post(url, files=files, headers=headers, timeout=180)
            
            if response.status_code == 200:
                response_data = response.json()
                print(f"✅ Upload successful - Analysis ID: {response_data.get('analysis_id')}")
                return True, response_data
            else:
                print(f"❌ Upload failed - Status: {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"   Error: {error_data}")
                except:
                    print(f"   Error: {response.text}")
                return False, None
                
        except Exception as e:
            print(f"❌ Upload error: {e}")
            return False, None

    def validate_extraction_quality(self, result_data):
        """Validate the extraction quality against expected data"""
        print("\n🔍 VALIDATING EXTRACTION QUALITY")
        print("=" * 60)
        
        result = result_data.get('result', {})
        validation_results = {
            "header_data": False,
            "price_data": False,
            "money_box": False,
            "conformita": False,
            "occupazione": False,
            "legal_killers": False,
            "evidence_tracking": False,
            "page_references": False
        }
        
        issues_found = []
        
        # 1. Validate Header Data
        print("\n1️⃣ HEADER DATA VALIDATION")
        report_header = result.get('report_header', {})
        
        procedure_found = False
        tribunale_found = False
        address_found = False
        lotto_found = False
        
        # Check procedure
        procedure_data = report_header.get('procedure', {})
        if isinstance(procedure_data, dict):
            procedure_value = procedure_data.get('value', '')
            if '62/2024' in procedure_value or 'R.G.E.' in procedure_value:
                procedure_found = True
                print(f"   ✅ Procedure found: {procedure_value}")
            else:
                print(f"   ❌ Procedure not found or incorrect: {procedure_value}")
        
        # Check tribunale
        tribunale_data = report_header.get('tribunale', {})
        if isinstance(tribunale_data, dict):
            tribunale_value = tribunale_data.get('value', '')
            if 'MANTOVA' in tribunale_value.upper():
                tribunale_found = True
                print(f"   ✅ Tribunale found: {tribunale_value}")
            else:
                print(f"   ❌ Tribunale not found or incorrect: {tribunale_value}")
        
        # Check address
        address_data = report_header.get('address', {})
        if isinstance(address_data, dict):
            address_value = address_data.get('value', '')
            if 'Sordello' in address_value or 'San Giorgio Bigarello' in address_value:
                address_found = True
                print(f"   ✅ Address found: {address_value}")
            else:
                print(f"   ❌ Address not found or incorrect: {address_value}")
        
        # Check lotto
        lotto_data = report_header.get('lotto', {})
        if isinstance(lotto_data, dict):
            lotto_value = lotto_data.get('value', '')
            if 'Unico' in lotto_value:
                lotto_found = True
                print(f"   ✅ Lotto found: {lotto_value}")
            else:
                print(f"   ❌ Lotto not found or incorrect: {lotto_value}")
        
        validation_results["header_data"] = procedure_found and tribunale_found and address_found and lotto_found
        
        # 2. Validate Price Data
        print("\n2️⃣ PRICE DATA VALIDATION")
        dati_certi = result.get('section_4_dati_certi', {})
        
        prezzo_base_found = False
        valore_stima_found = False
        
        # Check prezzo base d'asta
        prezzo_base = dati_certi.get('prezzo_base_asta', {})
        if isinstance(prezzo_base, dict):
            prezzo_value = prezzo_base.get('value', 0)
            if abs(prezzo_value - self.expected_data['prices']['prezzo_base_asta']) < 1000:
                prezzo_base_found = True
                print(f"   ✅ Prezzo base found: €{prezzo_value:,.2f}")
            else:
                print(f"   ❌ Prezzo base incorrect: €{prezzo_value:,.2f} (expected €{self.expected_data['prices']['prezzo_base_asta']:,.2f})")
        
        # Check valore di stima
        valore_stima = dati_certi.get('valore_stima_complessivo', {})
        if isinstance(valore_stima, dict):
            stima_value = valore_stima.get('value', 0)
            if abs(stima_value - self.expected_data['prices']['valore_stima']) < 1000:
                valore_stima_found = True
                print(f"   ✅ Valore stima found: €{stima_value:,.2f}")
            else:
                print(f"   ❌ Valore stima incorrect: €{stima_value:,.2f} (expected €{self.expected_data['prices']['valore_stima']:,.2f})")
        
        validation_results["price_data"] = prezzo_base_found and valore_stima_found
        
        # 3. Validate Money Box
        print("\n3️⃣ MONEY BOX VALIDATION")
        money_box = result.get('section_3_money_box', {})
        money_box_items = money_box.get('items', [])
        
        money_box_valid = len(money_box_items) >= 6
        regolarizzazione_found = False
        completamento_found = False
        
        for item in money_box_items:
            voce = item.get('voce', '')
            stima_euro = item.get('stima_euro', 0)
            
            if 'Regolarizzazione' in voce and stima_euro == 23000:
                regolarizzazione_found = True
                print(f"   ✅ Regolarizzazione urbanistica: €{stima_euro}")
            elif 'Completamento' in voce and stima_euro == 15000:
                completamento_found = True
                print(f"   ✅ Completamento finiture: €{stima_euro}")
        
        validation_results["money_box"] = money_box_valid and regolarizzazione_found and completamento_found
        
        # 4. Validate Evidence Tracking
        print("\n4️⃣ EVIDENCE TRACKING VALIDATION")
        evidence_count = 0
        page_references_found = False
        
        # Count evidence entries across all sections
        for section_key, section_data in result.items():
            if isinstance(section_data, dict):
                evidence_count += self._count_evidence_in_section(section_data)
        
        if evidence_count >= 5:
            print(f"   ✅ Evidence entries found: {evidence_count}")
            validation_results["evidence_tracking"] = True
        else:
            print(f"   ❌ Insufficient evidence entries: {evidence_count} (expected ≥5)")
        
        # Check for page references
        result_str = json.dumps(result)
        if 'p. 40' in result_str or 'p. 45' in result_str or 'p. 35' in result_str:
            page_references_found = True
            print(f"   ✅ Page references found in response")
        else:
            print(f"   ❌ No page references found")
        
        validation_results["page_references"] = page_references_found
        
        # 5. Validate Conformità Section
        print("\n5️⃣ CONFORMITÀ VALIDATION")
        conformita = result.get('section_5_abusi_conformita', {})
        
        conformita_urbanistica = conformita.get('conformita_urbanistica', {})
        agibilita = conformita.get('agibilita', {})
        
        conformita_valid = False
        agibilita_valid = False
        
        if conformita_urbanistica.get('status') == 'CONFORME':
            conformita_valid = True
            print(f"   ✅ Conformità urbanistica: {conformita_urbanistica.get('status')}")
        
        if agibilita.get('status') in ['ASSENTE', 'NON_RISULTA']:
            agibilita_valid = True
            print(f"   ✅ Agibilità: {agibilita.get('status')}")
        
        validation_results["conformita"] = conformita_valid and agibilita_valid
        
        # 6. Validate Occupazione
        print("\n6️⃣ OCCUPAZIONE VALIDATION")
        occupazione = result.get('section_6_stato_occupativo', {})
        
        if occupazione.get('status') == 'OCCUPATO_DEBITORE':
            validation_results["occupazione"] = True
            print(f"   ✅ Stato occupativo: {occupazione.get('status')}")
        else:
            print(f"   ❌ Stato occupativo incorrect: {occupazione.get('status')}")
        
        # 7. Validate Legal Killers
        print("\n7️⃣ LEGAL KILLERS VALIDATION")
        legal_killers = result.get('section_9_legal_killers', {})
        legal_items = legal_killers.get('items', [])
        
        if len(legal_items) >= 8:
            validation_results["legal_killers"] = True
            print(f"   ✅ Legal killers checklist: {len(legal_items)} items")
        else:
            print(f"   ❌ Legal killers incomplete: {len(legal_items)} items (expected ≥8)")
        
        # Summary
        print(f"\n📊 VALIDATION SUMMARY")
        print("=" * 40)
        passed_validations = sum(validation_results.values())
        total_validations = len(validation_results)
        
        for key, passed in validation_results.items():
            status = "✅" if passed else "❌"
            print(f"{status} {key.replace('_', ' ').title()}")
        
        print(f"\n🎯 Overall Score: {passed_validations}/{total_validations} ({passed_validations/total_validations*100:.1f}%)")
        
        # Determine if extraction quality is acceptable
        critical_validations = ["header_data", "price_data", "evidence_tracking"]
        critical_passed = sum(validation_results[key] for key in critical_validations)
        
        if critical_passed == len(critical_validations) and passed_validations >= 6:
            print("✅ EXTRACTION QUALITY: EXCELLENT")
            return True, validation_results
        elif critical_passed >= 2 and passed_validations >= 4:
            print("⚠️ EXTRACTION QUALITY: ACCEPTABLE")
            return True, validation_results
        else:
            print("❌ EXTRACTION QUALITY: POOR")
            return False, validation_results

    def _count_evidence_in_section(self, section_data):
        """Recursively count evidence entries in a section"""
        count = 0
        
        if isinstance(section_data, dict):
            if 'evidence' in section_data:
                evidence = section_data['evidence']
                if isinstance(evidence, list):
                    for ev in evidence:
                        if isinstance(ev, dict) and 'page' in ev and 'quote' in ev:
                            count += 1
            
            # Recursively check nested structures
            for value in section_data.values():
                if isinstance(value, (dict, list)):
                    count += self._count_evidence_in_section(value)
        
        elif isinstance(section_data, list):
            for item in section_data:
                if isinstance(item, (dict, list)):
                    count += self._count_evidence_in_section(item)
        
        return count

    def run_comprehensive_test(self):
        """Run the comprehensive perizia extraction test"""
        print("🚀 NEXODIFY FORENSIC ENGINE - PERIZIA EXTRACTION TEST")
        print("🎯 Testing IMPROVED PDF extraction (pdfplumber + pymupdf)")
        print("📄 Document: f6h0fsye_1859886_c_perizia.pdf")
        print("=" * 80)
        
        # Step 1: Create test user
        session_token, user_id = self.create_test_user_and_session()
        if not session_token:
            print("❌ CRITICAL FAILURE: Could not create test user")
            return False
        
        # Step 2: Upload and analyze the perizia document
        success, response_data = self.upload_perizia_document()
        if not success:
            print("❌ CRITICAL FAILURE: Could not upload perizia document")
            return False
        
        # Step 3: Validate extraction quality
        extraction_valid, validation_results = self.validate_extraction_quality(response_data)
        
        # Step 4: Save detailed results
        test_results = {
            "timestamp": datetime.now().isoformat(),
            "test_type": "perizia_extraction_quality",
            "document": "f6h0fsye_1859886_c_perizia.pdf",
            "extraction_valid": extraction_valid,
            "validation_results": validation_results,
            "response_data": response_data,
            "expected_data": self.expected_data
        }
        
        with open('/app/perizia_extraction_results.json', 'w') as f:
            json.dump(test_results, f, indent=2)
        
        print(f"\n📄 Detailed results saved to /app/perizia_extraction_results.json")
        
        return extraction_valid

def main():
    tester = PeriziaExtractionTester()
    success = tester.run_comprehensive_test()
    
    if success:
        print("\n✅ PERIZIA EXTRACTION TEST PASSED")
        return 0
    else:
        print("\n❌ PERIZIA EXTRACTION TEST FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())