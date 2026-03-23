import sys
import os

# Add project root to sys.path so that 'services', 'dashboard_app', etc. can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
