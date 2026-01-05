#!/usr/bin/env python3

import sys
import os
sys.path.append('/app')

from backend_test import NexodifyAPITester, create_test_user_and_session

def main():
    print("🚀 Quick Test: MULTI-LOT and CORRECTNESS PATCHES")
    print("=" * 60)
    
    # Create test user and session
    session_token, user_id = create_test_user_and_session()
    
    if not session_token:
        print("❌ Failed to create session token")
        return 1
    
    # Initialize tester
    tester = NexodifyAPITester()
    tester.token = session_token
    
    # Run the main test
    print("\n🎯 MAIN TEST: REVIEW REQUEST - MULTI-LOT and CORRECTNESS PATCHES...")
    success, response_data = tester.test_review_request_multi_lot_correctness_patches()
    
    if success:
        print("\n✅ REVIEW REQUEST TEST PASSED")
        return 0
    else:
        print("\n❌ REVIEW REQUEST TEST FAILED")
        if tester.critical_failures:
            print(f"\n🚨 CRITICAL FAILURES ({len(tester.critical_failures)}):")
            for failure in tester.critical_failures:
                print(f"   ❌ {failure.get('test', 'Unknown')}")
                if 'issue' in failure:
                    print(f"      Issue: {failure['issue']}")
        return 1

if __name__ == "__main__":
    sys.exit(main())