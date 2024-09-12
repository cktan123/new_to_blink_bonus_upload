import psutil
import os
import json
from datetime import datetime, timedelta

def save_state(state, filename="cpu_state.json"):
    with open(filename, 'w') as f:
        json.dump(state, f)

def load_state(filename="cpu_state.json"):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def check_shutdown(threshold=10, inactivity_period=3600, state_file="cpu_state.json"):
    cpu_usage = psutil.cpu_percent(interval=1)
    print(f"Current CPU usage: {cpu_usage}%")
    
    state = load_state(state_file)
    current_time = datetime.now()
    
    if state is not None:
        last_check_time = datetime.fromisoformat(state['last_check'])
        inactive_since = datetime.fromisoformat(state['inactive_since']) if state['inactive'] else current_time
        
        if cpu_usage < threshold:
            if not state['inactive']:
                state['inactive'] = True
                state['inactive_since'] = current_time.isoformat()
        else:
            state['inactive'] = False
            state['inactive_since'] = None
        
        if state['inactive'] and current_time - inactive_since > timedelta(seconds=inactivity_period):
            print(f"CPU has been inactive for {inactivity_period}secs, shutting down VM...")
            os.system('sudo /sbin/shutdown -h now')
        
    else:
        state = {
            'last_check': current_time.isoformat(),
            'inactive': cpu_usage < threshold,
            'inactive_since': current_time.isoformat() if cpu_usage < threshold else None
        }
    
    state['last_check'] = current_time.isoformat()
    save_state(state, state_file)