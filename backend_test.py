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
    
    if session_token:
        tester.test_auth_me_with_token(session_token)
        tester.test_dashboard_stats()
    else:
        print("⚠️ Skipping authenticated tests - no session token")
    
    # Test protected endpoints without auth
    print("\n🛡️ Testing Protected Endpoints (No Auth)...")
    tester.test_perizia_upload_no_auth()
    tester.test_assistant_no_auth()
    
    # Print results
    print(f"\n📊 Test Results: {tester.tests_passed}/{tester.tests_run} passed")
    
    # Save detailed results
    results = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": tester.tests_run,
        "passed_tests": tester.tests_passed,
        "success_rate": (tester.tests_passed / tester.tests_run * 100) if tester.tests_run > 0 else 0,
        "test_details": tester.test_results
    }
    
    with open('/app/backend_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"📄 Detailed results saved to /app/backend_test_results.json")
    
    return 0 if tester.tests_passed == tester.tests_run else 1

if __name__ == "__main__":
    sys.exit(main())