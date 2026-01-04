import requests
import sys
import json
from datetime import datetime
import os
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

class NexodifyAPITester:
    def __init__(self, base_url="https://property-analyzer-10.preview.emergentagent.com"):
        self.base_url = base_url
        self.token = None
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        self.critical_failures = []

    def create_test_pdf(self):
        """Create a test PDF with Italian legal document content"""
        buffer = io.BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        
        # Page 1 - Header and basic info
        p.drawString(100, 750, "TRIBUNALE DI ROMA")
        p.drawString(100, 730, "SEZIONE ESECUZIONI IMMOBILIARI")
        p.drawString(100, 710, "R.G.E. N. 123/2024")
        p.drawString(100, 690, "Lotto Unico")
        p.drawString(100, 670, "Procedura di Esecuzione Immobiliare")
        p.drawString(100, 650, "Depositata il: 15/03/2024")
        
        # Address and property details
        p.drawString(100, 620, "IMMOBILE SITO IN:")
        p.drawString(100, 600, "Via Roma 123, 00100 Roma (RM)")
        p.drawString(100, 580, "Superficie catastale: 85 mq")
        p.drawString(100, 560, "Categoria: A/2, Classe: 3, Vani: 4")
        
        # Price information
        p.drawString(100, 520, "PREZZO BASE D'ASTA:")
        p.drawString(100, 500, "Il prezzo base d'asta è fissato in € 150.000,00")
        p.drawString(100, 480, "(centocinquantamila/00)")
        
        p.showPage()
        
        # Page 2 - Conformity and occupancy
        p.drawString(100, 750, "CONFORMITÀ URBANISTICA E CATASTALE")
        p.drawString(100, 720, "L'immobile risulta conforme alla documentazione urbanistica")
        p.drawString(100, 700, "depositata presso il Comune di Roma.")
        p.drawString(100, 680, "Conformità catastale: CONFORME")
        
        p.drawString(100, 640, "STATO OCCUPATIVO:")
        p.drawString(100, 620, "L'immobile risulta LIBERO da persone e cose")
        p.drawString(100, 600, "alla data del sopralluogo del 10/03/2024.")
        
        p.drawString(100, 560, "FORMALITÀ:")
        p.drawString(100, 540, "Ipoteca iscritta per € 200.000,00")
        p.drawString(100, 520, "Pignoramento trascritto in data 01/02/2024")
        
        p.showPage()
        p.save()
        
        buffer.seek(0)
        return buffer.getvalue()

    def run_test(self, name, method, endpoint, expected_status, data=None, headers=None):
        """Run a single API test"""
        url = f"{self.base_url}/{endpoint}"
        test_headers = {'Content-Type': 'application/json'}
        if headers:
            test_headers.update(headers)
        if self.token:
            test_headers['Authorization'] = f'Bearer {self.token}'

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=test_headers, timeout=30)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=test_headers, timeout=30)
            elif method == 'DELETE':
                response = requests.delete(url, headers=test_headers, timeout=30)

            success = response.status_code == expected_status
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                try:
                    response_data = response.json()
                except:
                    response_data = {}
            else:
                print(f"❌ Failed - Expected {expected_status}, got {response.status_code}")
                try:
                    response_data = response.json()
                    print(f"   Response: {response_data}")
                except:
                    response_data = {"error": response.text}

            self.test_results.append({
                "test": name,
                "method": method,
                "endpoint": endpoint,
                "expected_status": expected_status,
                "actual_status": response.status_code,
                "success": success,
                "response": response_data
            })

            return success, response_data

        except Exception as e:
            print(f"❌ Failed - Error: {str(e)}")
            self.test_results.append({
                "test": name,
                "method": method,
                "endpoint": endpoint,
                "expected_status": expected_status,
                "actual_status": "ERROR",
                "success": False,
                "error": str(e)
            })
            return False, {}

    def run_file_upload_test(self, name, endpoint, expected_status, files, headers=None):
        """Run a file upload test"""
        url = f"{self.base_url}/{endpoint}"
        test_headers = {}
        if headers:
            test_headers.update(headers)
        if self.token:
            test_headers['Authorization'] = f'Bearer {self.token}'

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        
        try:
            response = requests.post(url, files=files, headers=test_headers, timeout=120)

            success = response.status_code == expected_status
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                try:
                    response_data = response.json()
                except:
                    response_data = {}
            else:
                print(f"❌ Failed - Expected {expected_status}, got {response.status_code}")
                try:
                    response_data = response.json()
                    print(f"   Response: {response_data}")
                except:
                    response_data = {"error": response.text}
                
                # Track critical failures
                if "CRITICAL" in name.upper() or "EVIDENCE" in name.upper():
                    self.critical_failures.append({
                        "test": name,
                        "expected": expected_status,
                        "actual": response.status_code,
                        "response": response_data
                    })

            self.test_results.append({
                "test": name,
                "method": "POST",
                "endpoint": endpoint,
                "expected_status": expected_status,
                "actual_status": response.status_code,
                "success": success,
                "response": response_data
            })

            return success, response_data

        except Exception as e:
            print(f"❌ Failed - Error: {str(e)}")
            error_result = {
                "test": name,
                "method": "POST",
                "endpoint": endpoint,
                "expected_status": expected_status,
                "actual_status": "ERROR",
                "success": False,
                "error": str(e)
            }
            self.test_results.append(error_result)
            
            if "CRITICAL" in name.upper() or "EVIDENCE" in name.upper():
                self.critical_failures.append(error_result)
            
            return False, {}

    def test_perizia_analysis_with_evidence(self):
        """CRITICAL TEST: Test perizia analysis with evidence extraction"""
        if not self.token:
            print("⚠️ Skipping perizia test - no authentication token")
            return False, {}
        
        print("📄 Creating test PDF with Italian legal content...")
        pdf_content = self.create_test_pdf()
        
        files = {
            'file': ('test_perizia.pdf', pdf_content, 'application/pdf')
        }
        
        success, response_data = self.run_file_upload_test(
            "CRITICAL: Perizia Analysis with Evidence", 
            "api/analysis/perizia", 
            200, 
            files
        )
        
        if success and response_data:
            # Verify evidence structure in response
            result = response_data.get('result', {})
            evidence_found = False
            evidence_details = []
            
            # Check case_header for evidence
            case_header = result.get('case_header', {})
            for field, data in case_header.items():
                if isinstance(data, dict) and 'evidence' in data:
                    evidence_array = data.get('evidence', [])
                    if evidence_array:
                        evidence_found = True
                        for ev in evidence_array:
                            if 'page' in ev and 'quote' in ev:
                                evidence_details.append(f"{field}: page {ev['page']}, quote: '{ev['quote'][:50]}...'")
            
            # Check dati_certi_del_lotto for evidence
            dati_certi = result.get('dati_certi_del_lotto', {})
            for field, data in dati_certi.items():
                if isinstance(data, dict) and 'evidence' in data:
                    evidence_array = data.get('evidence', [])
                    if evidence_array:
                        evidence_found = True
                        for ev in evidence_array:
                            if 'page' in ev and 'quote' in ev:
                                evidence_details.append(f"{field}: page {ev['page']}, quote: '{ev['quote'][:50]}...'")
            
            if evidence_found:
                print(f"✅ EVIDENCE FOUND: {len(evidence_details)} evidence entries")
                for detail in evidence_details[:3]:  # Show first 3
                    print(f"   📍 {detail}")
            else:
                print("❌ CRITICAL FAILURE: No evidence arrays with page/quote found in response")
                self.critical_failures.append({
                    "test": "Evidence Structure Validation",
                    "issue": "No evidence arrays with page/quote found",
                    "response_structure": list(result.keys())
                })
            
            return success, response_data
        
        return success, response_data

    def test_google_docai_perizia_extraction(self):
        """CRITICAL TEST: Test Google Document AI OCR integration with specific perizia document"""
        if not self.token:
            print("⚠️ Skipping Google Document AI test - no authentication token")
            return False, {}
        
        print("🎯 CRITICAL TEST: Google Document AI OCR Integration")
        print("📄 Downloading specific perizia document for testing...")
        
        # Download the specific test document
        test_pdf_url = "https://customer-assets.emergentagent.com/job_property-analyzer-10/artifacts/f6h0fsye_1859886_c_perizia.pdf"
        
        try:
            import requests
            pdf_response = requests.get(test_pdf_url, timeout=60)
            if pdf_response.status_code != 200:
                print(f"❌ Failed to download test PDF: {pdf_response.status_code}")
                return False, {}
            
            pdf_content = pdf_response.content
            print(f"✅ Downloaded PDF: {len(pdf_content)} bytes")
            
        except Exception as e:
            print(f"❌ Error downloading test PDF: {e}")
            return False, {}
        
        files = {
            'file': ('f6h0fsye_1859886_c_perizia.pdf', pdf_content, 'application/pdf')
        }
        
        success, response_data = self.run_file_upload_test(
            "CRITICAL: Google Document AI Perizia Extraction", 
            "api/analysis/perizia", 
            200, 
            files
        )
        
        if success and response_data:
            result = response_data.get('result', {})
            
            # Verify ROMA STANDARD structure
            expected_sections = [
                'section_1_semaforo_generale',
                'section_2_decisione_rapida', 
                'section_3_money_box',
                'section_4_dati_certi',
                'section_5_abusi_conformita',
                'section_12_checklist_pre_offerta',
                'qa_pass'
            ]
            
            missing_sections = []
            for section in expected_sections:
                if section not in result:
                    missing_sections.append(section)
            
            if missing_sections:
                print(f"❌ MISSING SECTIONS: {missing_sections}")
                self.critical_failures.append({
                    "test": "ROMA STANDARD Structure",
                    "issue": f"Missing sections: {missing_sections}",
                    "expected": expected_sections,
                    "found": list(result.keys())
                })
            else:
                print("✅ ROMA STANDARD structure complete")
            
            # Verify specific expected data from the review request
            verification_results = []
            
            # Check report_header data
            report_header = result.get('report_header', {})
            procedure = report_header.get('procedure', {}).get('value', '')
            if 'Esecuzione Immobiliare 62/2024' in procedure or '62/2024' in procedure:
                verification_results.append("✅ Procedure ID: Found R.G.E. 62/2024")
            else:
                verification_results.append(f"❌ Procedure ID: Expected '62/2024', got '{procedure}'")
            
            tribunale = report_header.get('tribunale', {}).get('value', '')
            if 'MANTOVA' in tribunale.upper():
                verification_results.append("✅ Tribunale: Found TRIBUNALE DI MANTOVA")
            else:
                verification_results.append(f"❌ Tribunale: Expected 'MANTOVA', got '{tribunale}'")
            
            address = report_header.get('address', {}).get('value', '')
            if 'Sordello' in address and 'San Giorgio Bigarello' in address:
                verification_results.append("✅ Address: Found Via Sordello, San Giorgio Bigarello")
            else:
                verification_results.append(f"❌ Address: Expected 'Via Sordello, San Giorgio Bigarello', got '{address}'")
            
            # Check section_4_dati_certi
            dati_certi = result.get('section_4_dati_certi', {})
            prezzo_base = dati_certi.get('prezzo_base_asta', {}).get('value', 0)
            if prezzo_base == 391849:
                verification_results.append("✅ Prezzo Base: Found €391,849.00")
            else:
                verification_results.append(f"❌ Prezzo Base: Expected 391849, got {prezzo_base}")
            
            valore_finale = dati_certi.get('valore_finale_stima', {}).get('value', 0)
            if valore_finale == 391849:
                verification_results.append("✅ Valore Finale: Found €391,849.00")
            else:
                verification_results.append(f"❌ Valore Finale: Expected 391849, got {valore_finale}")
            
            # Check section_3_money_box for specific amounts
            money_box = result.get('section_3_money_box', {})
            items = money_box.get('items', [])
            
            found_23000 = False
            found_5000 = False
            for item in items:
                stima_euro = item.get('stima_euro', 0)
                if stima_euro == 23000:
                    found_23000 = True
                    verification_results.append("✅ Money Box: Found €23,000 (regolarizzazione urbanistica)")
                elif stima_euro == 5000:
                    found_5000 = True
                    verification_results.append("✅ Money Box: Found €5,000 (vizi occulti)")
            
            if not found_23000:
                verification_results.append("❌ Money Box: Missing €23,000 regolarizzazione urbanistica")
            if not found_5000:
                verification_results.append("❌ Money Box: Missing €5,000 vizi occulti")
            
            # Check section_5_abusi_conformita
            conformita = result.get('section_5_abusi_conformita', {})
            
            agibilita = conformita.get('agibilita', {}).get('status', '')
            if agibilita in ['ASSENTE', 'NON_RISULTA']:
                verification_results.append("✅ Agibilità: Correctly identified as ASSENTE/NON_RISULTA")
            else:
                verification_results.append(f"❌ Agibilità: Expected ASSENTE/NON_RISULTA, got '{agibilita}'")
            
            ape = conformita.get('ape', {}).get('status', '')
            if ape == 'ASSENTE':
                verification_results.append("✅ APE: Correctly identified as ASSENTE")
            else:
                verification_results.append(f"❌ APE: Expected ASSENTE, got '{ape}'")
            
            impianti = conformita.get('impianti', {})
            elettrico = impianti.get('elettrico', {}).get('conformita', '')
            termico = impianti.get('termico', {}).get('conformita', '')
            idrico = impianti.get('idrico', {}).get('conformita', '')
            
            if elettrico == 'NO':
                verification_results.append("✅ Impianto Elettrico: Correctly identified as NO")
            else:
                verification_results.append(f"❌ Impianto Elettrico: Expected NO, got '{elettrico}'")
            
            if termico == 'NO':
                verification_results.append("✅ Impianto Termico: Correctly identified as NO")
            else:
                verification_results.append(f"❌ Impianto Termico: Expected NO, got '{termico}'")
            
            if idrico == 'NO':
                verification_results.append("✅ Impianto Idrico: Correctly identified as NO")
            else:
                verification_results.append(f"❌ Impianto Idrico: Expected NO, got '{idrico}'")
            
            # Check section_12_checklist_pre_offerta
            checklist = result.get('section_12_checklist_pre_offerta', [])
            if len(checklist) >= 5:
                verification_results.append(f"✅ Checklist: Found {len(checklist)} items (expected 5)")
            else:
                verification_results.append(f"❌ Checklist: Found {len(checklist)} items, expected 5")
            
            # Check qa_pass
            qa_pass = result.get('qa_pass', {})
            checks = qa_pass.get('checks', [])
            if len(checks) >= 9:
                verification_results.append(f"✅ QA Pass: Found {len(checks)} checks (expected 9)")
            else:
                verification_results.append(f"❌ QA Pass: Found {len(checks)} checks, expected 9")
            
            # Print verification results
            print("\n📊 VERIFICATION RESULTS:")
            passed_verifications = 0
            total_verifications = len(verification_results)
            
            for result_line in verification_results:
                print(f"   {result_line}")
                if result_line.startswith("✅"):
                    passed_verifications += 1
            
            extraction_quality = (passed_verifications / total_verifications * 100) if total_verifications > 0 else 0
            print(f"\n📈 EXTRACTION QUALITY: {extraction_quality:.1f}% ({passed_verifications}/{total_verifications})")
            
            # Check for evidence tracking
            evidence_count = 0
            for section_name, section_data in result.items():
                if isinstance(section_data, dict):
                    for field_name, field_data in section_data.items():
                        if isinstance(field_data, dict) and 'evidence' in field_data:
                            evidence_array = field_data.get('evidence', [])
                            evidence_count += len(evidence_array)
            
            print(f"📍 EVIDENCE TRACKING: {evidence_count} evidence entries found")
            
            # Determine if test passed
            critical_data_found = (
                '62/2024' in procedure and
                'MANTOVA' in tribunale.upper() and
                prezzo_base == 391849 and
                len(checklist) >= 5 and
                len(checks) >= 9
            )
            
            if critical_data_found and extraction_quality >= 60:
                print("✅ GOOGLE DOCUMENT AI TEST PASSED - Critical data extracted successfully")
                return True, response_data
            else:
                print("❌ GOOGLE DOCUMENT AI TEST FAILED - Critical data missing or extraction quality too low")
                self.critical_failures.append({
                    "test": "Google Document AI Extraction",
                    "issue": f"Extraction quality {extraction_quality:.1f}% or critical data missing",
                    "verification_results": verification_results
                })
                return False, response_data
        
        return success, response_data

    def test_session_auth_flow(self):
        """Test session-based authentication flow"""
        # This would normally require a real session_id from Emergent Auth
        # For testing, we'll simulate with a mock session
        mock_session_data = {
            "session_id": "mock_session_123"
        }
        
        return self.run_test(
            "Session Auth Flow", 
            "POST", 
            "api/auth/session", 
            401,  # Expected to fail with mock data
            data=mock_session_data
        )

    def test_assistant_with_context(self):
        """Test assistant with perizia context"""
        if not self.token:
            print("⚠️ Skipping assistant test - no authentication token")
            return False, {}
        
        assistant_data = {
            "question": "Quali sono i rischi principali di questo immobile?",
            "related_case_id": None
        }
        
        return self.run_test(
            "Assistant Q&A", 
            "POST", 
            "api/analysis/assistant", 
            200, 
            data=assistant_data
        )

    def test_health_check(self):
        """Test health endpoint"""
        return self.run_test("Health Check", "GET", "api/health", 200)

    def test_get_plans(self):
        """Test subscription plans endpoint"""
        return self.run_test("Get Plans", "GET", "api/plans", 200)

    def test_auth_me_without_token(self):
        """Test auth/me without token (should fail)"""
        return self.run_test("Auth Me (No Token)", "GET", "api/auth/me", 401)

    def test_auth_me_with_token(self, token):
        """Test auth/me with token"""
        self.token = token
        return self.run_test("Auth Me (With Token)", "GET", "api/auth/me", 200)

    def test_dashboard_stats(self):
        """Test dashboard stats (requires auth)"""
        return self.run_test("Dashboard Stats", "GET", "api/dashboard/stats", 200)

    def test_perizia_upload_no_auth(self):
        """Test perizia upload without auth (should fail)"""
        old_token = self.token
        self.token = None
        result = self.run_test("Perizia Upload (No Auth)", "POST", "api/analysis/perizia", 401)
        self.token = old_token
        return result

    def test_assistant_no_auth(self):
        """Test assistant without auth (should fail)"""
        old_token = self.token
        self.token = None
        result = self.run_test("Assistant (No Auth)", "POST", "api/analysis/assistant", 401, 
                              data={"question": "Test question"})
        self.token = old_token
        return result

    # ===================
    # DELETE ENDPOINT TESTS
    # ===================

    def test_delete_endpoints_comprehensive(self):
        """Comprehensive test suite for all DELETE endpoints"""
        if not self.token:
            print("⚠️ Skipping DELETE tests - no authentication token")
            return False
        
        print("\n🗑️ TESTING DELETE ENDPOINTS - Nexodify Forensic Engine")
        print("=" * 60)
        
        # Store original test counts for DELETE-specific reporting
        delete_tests_start = self.tests_run
        delete_tests_passed_start = self.tests_passed
        
        # Test 1: DELETE endpoints without authentication (should return 401)
        print("\n🔒 Testing DELETE endpoints without authentication...")
        self.test_delete_perizia_no_auth()
        self.test_delete_images_no_auth()
        self.test_delete_assistant_no_auth()
        self.test_delete_all_history_no_auth()
        
        # Test 2: DELETE non-existent items (should return 404)
        print("\n🔍 Testing DELETE endpoints with non-existent IDs...")
        self.test_delete_nonexistent_perizia()
        self.test_delete_nonexistent_images()
        self.test_delete_nonexistent_assistant()
        
        # Test 3: Create test data and then delete it
        print("\n📝 Creating test data for deletion tests...")
        perizia_id, image_id, qa_id = self.create_test_data_for_deletion()
        
        if perizia_id or image_id or qa_id:
            print("\n🗑️ Testing successful deletion of created data...")
            if perizia_id:
                self.test_delete_existing_perizia(perizia_id)
            if image_id:
                self.test_delete_existing_images(image_id)
            if qa_id:
                self.test_delete_existing_assistant(qa_id)
        
        # Test 4: Test delete all history
        print("\n🧹 Testing delete all history...")
        self.test_delete_all_history_success()
        
        # Calculate DELETE-specific results
        delete_tests_run = self.tests_run - delete_tests_start
        delete_tests_passed = self.tests_passed - delete_tests_passed_start
        
        print(f"\n📊 DELETE Tests Results: {delete_tests_passed}/{delete_tests_run} passed")
        
        return delete_tests_passed == delete_tests_run

    def test_delete_perizia_no_auth(self):
        """Test DELETE perizia without auth (should return 401)"""
        old_token = self.token
        self.token = None
        result = self.run_test(
            "DELETE Perizia (No Auth)", 
            "DELETE", 
            "api/analysis/perizia/test_id", 
            401
        )
        self.token = old_token
        return result

    def test_delete_images_no_auth(self):
        """Test DELETE images without auth (should return 401)"""
        old_token = self.token
        self.token = None
        result = self.run_test(
            "DELETE Images (No Auth)", 
            "DELETE", 
            "api/analysis/images/test_id", 
            401
        )
        self.token = old_token
        return result

    def test_delete_assistant_no_auth(self):
        """Test DELETE assistant without auth (should return 401)"""
        old_token = self.token
        self.token = None
        result = self.run_test(
            "DELETE Assistant (No Auth)", 
            "DELETE", 
            "api/analysis/assistant/test_id", 
            401
        )
        self.token = old_token
        return result

    def test_delete_all_history_no_auth(self):
        """Test DELETE all history without auth (should return 401)"""
        old_token = self.token
        self.token = None
        result = self.run_test(
            "DELETE All History (No Auth)", 
            "DELETE", 
            "api/history/all", 
            401
        )
        self.token = old_token
        return result

    def test_delete_nonexistent_perizia(self):
        """Test DELETE non-existent perizia (should return 404)"""
        return self.run_test(
            "DELETE Non-existent Perizia", 
            "DELETE", 
            "api/analysis/perizia/nonexistent_id_12345", 
            404
        )

    def test_delete_nonexistent_images(self):
        """Test DELETE non-existent images (should return 404)"""
        return self.run_test(
            "DELETE Non-existent Images", 
            "DELETE", 
            "api/analysis/images/nonexistent_id_12345", 
            404
        )

    def test_delete_nonexistent_assistant(self):
        """Test DELETE non-existent assistant (should return 404)"""
        return self.run_test(
            "DELETE Non-existent Assistant", 
            "DELETE", 
            "api/analysis/assistant/nonexistent_id_12345", 
            404
        )

    def create_test_data_for_deletion(self):
        """Create test data that can be deleted"""
        perizia_id = None
        image_id = None
        qa_id = None
        
        # Create test perizia analysis
        try:
            print("📄 Creating test perizia for deletion...")
            pdf_content = self.create_test_pdf()
            files = {'file': ('test_delete_perizia.pdf', pdf_content, 'application/pdf')}
            
            success, response_data = self.run_file_upload_test(
                "Create Test Perizia for Deletion", 
                "api/analysis/perizia", 
                200, 
                files
            )
            
            if success and response_data:
                perizia_id = response_data.get('analysis_id')
                print(f"✅ Created test perizia: {perizia_id}")
        except Exception as e:
            print(f"❌ Failed to create test perizia: {e}")
        
        # Create test image analysis
        try:
            print("🖼️ Creating test image analysis for deletion...")
            # Create a simple test image file
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_file:
                # Write minimal JPEG header
                tmp_file.write(b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.\' ",#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0\x00\x11\x08\x00\x01\x00\x01\x01\x01\x11\x00\x02\x11\x01\x03\x11\x01\xff\xc4\x00\x14\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\xff\xc4\x00\x14\x10\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xda\x00\x0c\x03\x01\x00\x02\x11\x03\x11\x00\x3f\x00\xaa\xff\xd9')
                tmp_file.flush()
                
                with open(tmp_file.name, 'rb') as img_file:
                    files = {'files': ('test_delete_image.jpg', img_file.read(), 'image/jpeg')}
                    
                    success, response_data = self.run_file_upload_test(
                        "Create Test Image for Deletion", 
                        "api/analysis/image", 
                        200, 
                        files
                    )
                    
                    if success and response_data:
                        # Extract forensics_id from response
                        result = response_data.get('result', {})
                        run_info = result.get('run', {})
                        image_id = f"forensics_{run_info.get('run_id', 'unknown')}"
                        print(f"✅ Created test image analysis: {image_id}")
                
                os.unlink(tmp_file.name)
        except Exception as e:
            print(f"❌ Failed to create test image analysis: {e}")
        
        # Create test assistant QA
        try:
            print("🤖 Creating test assistant QA for deletion...")
            assistant_data = {
                "question": "Test question for deletion test - what are the main risks?",
                "related_case_id": None
            }
            
            success, response_data = self.run_test(
                "Create Test Assistant QA for Deletion", 
                "POST", 
                "api/analysis/assistant", 
                200, 
                data=assistant_data
            )
            
            if success and response_data:
                result = response_data.get('result', {})
                run_info = result.get('run', {})
                qa_id = f"qa_{run_info.get('run_id', 'unknown')}"
                print(f"✅ Created test assistant QA: {qa_id}")
        except Exception as e:
            print(f"❌ Failed to create test assistant QA: {e}")
        
        return perizia_id, image_id, qa_id

    def test_delete_existing_perizia(self, perizia_id):
        """Test DELETE existing perizia (should return 200)"""
        success, response_data = self.run_test(
            f"DELETE Existing Perizia ({perizia_id})", 
            "DELETE", 
            f"api/analysis/perizia/{perizia_id}", 
            200
        )
        
        if success:
            # Verify the response contains success message
            if response_data.get('ok') and 'eliminata' in response_data.get('message', '').lower():
                print("✅ Perizia deletion confirmed with proper response")
            else:
                print("⚠️ Perizia deleted but response format unexpected")
        
        return success, response_data

    def test_delete_existing_images(self, image_id):
        """Test DELETE existing images (should return 200)"""
        success, response_data = self.run_test(
            f"DELETE Existing Images ({image_id})", 
            "DELETE", 
            f"api/analysis/images/{image_id}", 
            200
        )
        
        if success:
            # Verify the response contains success message
            if response_data.get('ok') and 'eliminata' in response_data.get('message', '').lower():
                print("✅ Image analysis deletion confirmed with proper response")
            else:
                print("⚠️ Image analysis deleted but response format unexpected")
        
        return success, response_data

    def test_delete_existing_assistant(self, qa_id):
        """Test DELETE existing assistant QA (should return 200)"""
        success, response_data = self.run_test(
            f"DELETE Existing Assistant QA ({qa_id})", 
            "DELETE", 
            f"api/analysis/assistant/{qa_id}", 
            200
        )
        
        if success:
            # Verify the response contains success message
            if response_data.get('ok') and 'eliminata' in response_data.get('message', '').lower():
                print("✅ Assistant QA deletion confirmed with proper response")
            else:
                print("⚠️ Assistant QA deleted but response format unexpected")
        
        return success, response_data

    def test_delete_all_history_success(self):
        """Test DELETE all history (should return 200 with counts)"""
        success, response_data = self.run_test(
            "DELETE All History", 
            "DELETE", 
            "api/history/all", 
            200
        )
        
        if success:
            # Verify the response contains deletion counts
            deleted_info = response_data.get('deleted', {})
            if isinstance(deleted_info, dict):
                perizia_count = deleted_info.get('perizia', 0)
                images_count = deleted_info.get('images', 0)
                assistant_count = deleted_info.get('assistant', 0)
                total_count = deleted_info.get('total', 0)
                
                print(f"✅ History deletion confirmed:")
                print(f"   📄 Perizia analyses: {perizia_count}")
                print(f"   🖼️ Image analyses: {images_count}")
                print(f"   🤖 Assistant QAs: {assistant_count}")
                print(f"   📊 Total deleted: {total_count}")
            else:
                print("⚠️ History deleted but response format unexpected")
        
        return success, response_data

def create_test_user_and_session():
    """Create test user and session in MongoDB"""
    import subprocess
    
    print("🔧 Creating test user and session in MongoDB...")
    
    mongo_script = """
    use('test_database');
    var userId = 'test-user-' + Date.now();
    var sessionToken = 'test_session_' + Date.now();
    db.users.insertOne({
      user_id: userId,
      email: 'test.user.' + Date.now() + '@example.com',
      name: 'Test User',
      picture: 'https://via.placeholder.com/150',
      plan: 'pro',
      is_master_admin: false,
      quota: {
        perizia_scans_remaining: 50,
        image_scans_remaining: 100,
        assistant_messages_remaining: 9999
      },
      created_at: new Date()
    });
    db.user_sessions.insertOne({
      session_id: 'sess_' + Date.now(),
      user_id: userId,
      session_token: sessionToken,
      expires_at: new Date(Date.now() + 7*24*60*60*1000),
      created_at: new Date()
    });
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
                print(f"✅ Created test user: {user_id}")
                print(f"✅ Created session token: {session_token}")
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

def main():
    print("🚀 Starting Nexodify Forensic Engine API Tests")
    print("🎯 FOCUS: DELETE Endpoints Testing")
    print("=" * 60)
    
    # Create test user and session
    session_token, user_id = create_test_user_and_session()
    
    # Initialize tester
    tester = NexodifyAPITester()
    
    # Run basic tests
    print("\n📋 Running Basic API Tests...")
    tester.test_health_check()
    tester.test_get_plans()
    
    # Test auth endpoints
    print("\n🔐 Testing Authentication...")
    tester.test_auth_me_without_token()
    tester.test_session_auth_flow()
    
    if session_token:
        tester.test_auth_me_with_token(session_token)
        tester.test_dashboard_stats()
        
        # MAIN FOCUS: Google Document AI OCR Integration Test
        print("\n🎯 MAIN FOCUS: Google Document AI OCR Integration Test...")
        tester.test_google_docai_perizia_extraction()
        
        # DELETE ENDPOINTS TESTING
        print("\n🗑️ DELETE ENDPOINTS TESTING...")
        tester.test_delete_endpoints_comprehensive()
        
        # Previous critical tests for regression
        print("\n🔄 Regression Tests - Previous Critical Features...")
        tester.test_perizia_analysis_with_evidence()
        tester.test_assistant_with_context()
        
    else:
        print("⚠️ Skipping authenticated tests - no session token")
    
    # Test protected endpoints without auth
    print("\n🛡️ Testing Protected Endpoints (No Auth)...")
    tester.test_perizia_upload_no_auth()
    tester.test_assistant_no_auth()
    
    # Print results
    print(f"\n📊 Test Results: {tester.tests_passed}/{tester.tests_run} passed")
    
    # Report critical failures
    if tester.critical_failures:
        print(f"\n🚨 CRITICAL FAILURES ({len(tester.critical_failures)}):")
        for failure in tester.critical_failures:
            print(f"   ❌ {failure.get('test', 'Unknown')}")
            if 'issue' in failure:
                print(f"      Issue: {failure['issue']}")
    else:
        print("\n✅ No critical failures detected")
    
    # Save detailed results
    results = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": tester.tests_run,
        "passed_tests": tester.tests_passed,
        "success_rate": (tester.tests_passed / tester.tests_run * 100) if tester.tests_run > 0 else 0,
        "critical_failures": tester.critical_failures,
        "test_details": tester.test_results
    }
    
    with open('/app/backend_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"📄 Detailed results saved to /app/backend_test_results.json")
    
    # Return exit code based on critical failures
    if tester.critical_failures:
        print("\n🚨 EXITING WITH ERROR - Critical failures detected")
        return 1
    elif tester.tests_passed == tester.tests_run:
        print("\n✅ ALL TESTS PASSED")
        return 0
    else:
        print(f"\n⚠️ SOME TESTS FAILED ({tester.tests_run - tester.tests_passed} failures)")
        return 1

if __name__ == "__main__":
    sys.exit(main())