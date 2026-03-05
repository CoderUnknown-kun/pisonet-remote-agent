# System Architecture

The Pisonet Remote Agent runs on each computer and communicates with a cloud backend.

Admin Dashboard (Web)
        │
        ▼
Firebase / Firestore
        │
        ▼
Pisonet Remote Agent (Python)

Agent Responsibilities
- System monitoring
- Remote command execution
- Screen streaming
- Auto update system
