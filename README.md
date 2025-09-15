# How Renewable Is My Energy? ETL Pipeline

A Python-based proof-of-concept ETL pipeline that fetches real-time carbon intensity data from the National Grid ESO API and stores it in a Supabase database, with a Streamlit dashboard for visualization.

The dashboard is viewable here: https://how-renewable-uk.streamlit.app/

(May take a few seconds to initialize when the dashboard first loads. This is a limitation of Streamlit's free hosting)

## Automated Workflow

The pipeline runs hourly via GitHub Actions, with daily cleanup of old data (to stay witin free hosting limits).
In production, this would be replaced by something like Apache Airflow
```mermaid
graph TB
    %% Data Sources
    A[External Energy Data APIs] --> B[Python ETL Script]
    
    %% GitHub Actions Workflow
    subgraph "GitHub Actions"
        B --> C[Data Extraction]
        C --> D[Data Transformation]
        D --> E[Data Validation]
        E --> F[Data Loading]
    end
    
    %% Database
    F --> G[(Supabase Database)]
    
    %% Dashboard
    G --> H[Streamlit App]
    H --> I[Energy Dashboard]
    
    %% Triggers
    J[Scheduled Trigger<br/>GitHub Actions] --> B
    K[Manual Trigger<br/>GitHub Actions] --> B
    
    %% User Interaction
    L[Users] --> I
    
    %% Styling
    classDef github fill:#2D3748,stroke:#4A5568,stroke-width:2px,color:#FFFFFF
    classDef database fill:#3182CE,stroke:#2C5282,stroke-width:2px,color:#FFFFFF
    classDef app fill:#38A169,stroke:#2F855A,stroke-width:2px,color:#FFFFFF
    classDef data fill:#E53E3E,stroke:#C53030,stroke-width:2px,color:#FFFFFF
    
    class B,C,D,E,F,J,K github
    class G database
    class H,I app
    class A,L data
```
    

## Project Structure

- `main.py` - ETL pipeline implementation
- `dashboard.py` - Streamlit dashboard
- `requirements.txt` - Python dependencies

## Run it yourself

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your Supabase credentials:
   ```
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_service_key
   ```

3. Run the ETL pipeline:
   ```
   python main.py
   ```

4. Start the dashboard:
   ```
   streamlit run dashboard.py
   ```
