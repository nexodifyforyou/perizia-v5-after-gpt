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
        
        success, response_data = self.run_test(
            "CRITICAL: Perizia Analysis with Evidence", 
            "POST", 
            "api/analysis/perizia", 
            200, 
            files=files
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
    print("🎯 FOCUS: Evidence Display Feature (P0 Critical)")
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
        
        # CRITICAL TESTS - Evidence Display Feature
        print("\n🎯 CRITICAL TESTS - Evidence Display Feature...")
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