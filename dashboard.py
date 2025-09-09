import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("SUPABASE_URL and/or SUPABASE_KEY environment variables not set")
    st.stop()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Define renewable energy sources
RENEWABLE_SOURCES = ['solar', 'wind', 'hydro', 'biomass']

# Set up the page
st.set_page_config(
    page_title="Renewable Energy Dashboard",
    page_icon="🌱",
    layout="wide"
)

st.title("🌱 How Renewable is My Energy?")

# Key metric - current renewable percentage
try:
    # Fetch latest generation mix data
    response = supabase.table('generation_mix').select('*').order('created_at', desc=True).limit(1).execute()
    
    if response.data:
        latest_data = response.data[0]
        generation_mix = latest_data['generation_mix']
        
        # Calculate renewable percentage
        df_gen = pd.DataFrame(generation_mix)
        renewable_df = df_gen[df_gen['fuel'].isin(RENEWABLE_SOURCES)]
        renewable_percentage = renewable_df['perc'].sum()

        # Display last updated time in British time
        updated_time = datetime.fromisoformat(latest_data['created_at'].replace('Z', '+00:00'))
        bst_time = updated_time.astimezone(pytz.timezone('Europe/London'))
        st.caption(f"Last updated: {bst_time.strftime('%B %d, %Y at %H:%M')} (GMT)")
except Exception as e:
    st.error(f"Error calculating renewable percentage: {str(e)}")

# Tabs for different views
tab1, tab2 = st.tabs(["National Data", "Regional Data"])

# Tab 1: National Data (Generation Mix and Trends)
with tab1:
    st.header("Energy Generation Mix")
    
    try:
        # Fetch generation mix data
        response = supabase.table('generation_mix').select('*').order('created_at', desc=True).limit(1).execute()
        
        if response.data:
            # Get the latest generation mix
            latest_data = response.data[0]
            generation_mix = latest_data['generation_mix']
            
            # Convert to DataFrame
            df = pd.DataFrame(generation_mix)
            df = df[df['perc'] > 0]  # Filter out zero percentages
            df = df.sort_values('perc', ascending=False)

            # Display metrics without delta arrows
            renewable_total = df[df['fuel'].isin(RENEWABLE_SOURCES)]['perc'].sum()
            non_renewable_total = df[~df['fuel'].isin(RENEWABLE_SOURCES)]['perc'].sum()
            
            st.subheader("Summary")
            col1, col2 = st.columns(2)
            col1.metric("Renewable Energy", f"{renewable_total:.1f}%", delta=None)
            col2.metric("Non-Renewable Energy", f"{non_renewable_total:.1f}%", delta=None)
            
            col1, col2 = st.columns(2)
            
            # Pie chart
            with col1:
                # Define colors: green shades for renewables, red shades for non-renewables
                renewable_colors = ['#2E8B57', '#32CD32', '#90EE90', '#006400']  # Greens for renewables
                non_renewable_colors = ['#DC143C', '#B22222', '#8B0000', '#CD5C5C', '#A52A2A']  # Different shades of red for non-renewables
                
                # Create a copy of the dataframe
                chart_df = df.copy()
                
                # Simple sorting by fuel type to achieve grouping
                # First separate renewables and non-renewables
                renewables_df = chart_df[chart_df['fuel'].isin(RENEWABLE_SOURCES)].copy()
                non_renewables_df = chart_df[~chart_df['fuel'].isin(RENEWABLE_SOURCES)].copy()
                
                # Sort each group by percentage (largest first)
                renewables_df = renewables_df.sort_values('perc', ascending=False)
                non_renewables_df = non_renewables_df.sort_values('perc', ascending=False)
                
                # Combine them with renewables first
                chart_df = pd.concat([renewables_df, non_renewables_df]).reset_index(drop=True)
                
                # Create color map based on fuel type
                color_map = {}
                renewable_idx = 0
                non_renewable_idx = 0
                
                for _, row in chart_df.iterrows():
                    fuel = row['fuel']
                    if fuel in RENEWABLE_SOURCES:
                        color_map[fuel] = renewable_colors[renewable_idx % len(renewable_colors)]
                        renewable_idx += 1
                    else:
                        color_map[fuel] = non_renewable_colors[non_renewable_idx % len(non_renewable_colors)]
                        non_renewable_idx += 1
                
                # Create pie chart with grouped colors
                fig_pie = px.pie(
                    chart_df,
                    values='perc',
                    names='fuel',
                    title=f"Live Energy Generation Mix<br><sup>Renewables (green) vs Non-renewables (red)</sup>",
                    color='fuel',
                    color_discrete_map=color_map,
                    hover_data=['perc']
                )
                
                # Simplify hover information to just show the fuel type name
                fig_pie.update_traces(
                    hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>"
                )
                
                # Remove any pull effects to keep the pie chart unified
                fig_pie.update_traces(pull=0)
                
                # Move legend closer to the pie chart
                fig_pie.update_layout(
                    legend=dict(
                        orientation="v",
                        yanchor="middle",
                        y=0.5,
                        xanchor="left",
                        x=0.95,  # Position closer to the chart
                        traceorder="normal",
                        font=dict(size=12)
                    ),
                    margin=dict(l=20, r=100, t=60, b=20)  # Adjust margins to accommodate legend
                )
                
                st.plotly_chart(fig_pie, use_container_width=True)
            
            # Time series chart
            with col2:
                # Renewable Energy Trends
                # Fetch historical generation mix data (last 30 days)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                start_datetime = start_date.isoformat()
                end_datetime = end_date.isoformat()
                
                response_trend = supabase.table('generation_mix').select('*').gte('created_at', start_datetime).lte('created_at', end_datetime).order('created_at').execute()
                
                if response_trend.data:
                    # Process data to calculate renewable percentages over time
                    trend_data = []
                    for record in response_trend.data:
                        gen_mix = record['generation_mix']
                        df_trend = pd.DataFrame(gen_mix)
                        renewable_df = df_trend[df_trend['fuel'].isin(RENEWABLE_SOURCES)]
                        renewable_pct = renewable_df['perc'].sum() if not renewable_df.empty else 0
                        
                        trend_data.append({
                            'timestamp': record['created_at'],
                            'renewable_percentage': renewable_pct
                        })
                    
                    # Convert to DataFrame and plot
                    df_trend = pd.DataFrame(trend_data)
                    df_trend['timestamp'] = pd.to_datetime(df_trend['timestamp'])
                    
                    # Create time series chart
                    fig_line = px.line(
                        df_trend,
                        x='timestamp',
                        y='renewable_percentage',
                        title="Renewable Energy Percentage Over Time (Last 30 Days)",
                        labels={'renewable_percentage': 'Renewable %', 'timestamp': 'Time'},
                        markers=True
                    )
                    fig_line.update_layout(hovermode="x unified")
                    
                    # Update hover template to show only the percentage
                    fig_line.update_traces(
                        hovertemplate="<b>%{y:.1f}%</b><extra></extra>"
                    )
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info("No historical data available for trends")
            
            # Display data table at the bottom with better styling
            st.header("Generation Mix Data")
            st.dataframe(
                df.style.format({"perc": "{:.1f}%"}), 
                use_container_width=True,
                height=400
            )
        else:
            st.info("No generation mix data available")
    except Exception as e:
        st.error(f"Error fetching generation mix data: {str(e)}")

# Tab 2: Regional Data
with tab2:
    st.header("Regional Renewable Energy Data")
    
    try:
        # Fetch regional data
        response = supabase.table('regional_intensity').select('*').order('created_at', desc=True).limit(1).execute()
        
        if response.data:
            # Get the latest regional data
            latest_data = response.data[0]
            regions = latest_data['regions']
            
            # Convert to DataFrame and calculate renewable percentages
            df_list = []
            for region in regions:
                # Calculate renewable percentage for this region
                renewable_pct = 0
                if 'generationmix' in region:
                    gen_mix_df = pd.DataFrame(region['generationmix'])
                    renewable_df = gen_mix_df[gen_mix_df['fuel'].isin(RENEWABLE_SOURCES)]
                    renewable_pct = renewable_df['perc'].sum() if not renewable_df.empty else 0
                
                df_list.append({
                    'region': region['shortname'],
                    'intensity_forecast': region['intensity']['forecast'],
                    'intensity_index': region['intensity']['index'],
                    'region_id': region['regionid'],
                    'renewable_percentage': renewable_pct
                })
            
            df = pd.DataFrame(df_list)
            df = df.sort_values('renewable_percentage', ascending=False)
            
            # Create bar chart for renewable percentages by region
            fig = px.bar(
                df,
                x='region',
                y='renewable_percentage',
                color='intensity_index',
                title=f"Renewable Energy Percentage by Region",
                labels={'renewable_percentage': 'Renewable %', 'region': 'Region'},
                color_discrete_map={
                    'very low': '#2E8B57',
                    'low': '#90EE90',
                    'moderate': '#FFD700',
                    'high': '#FF8C00',
                    'very high': '#FF0000'
                }
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Display metrics for top 5 regions by renewable energy
            st.subheader("Top 5 Regions by Renewable Energy")
            top5 = df.head(5)
            cols = st.columns(min(5, len(top5)))
            for i, (_, row) in enumerate(top5.iterrows()):
                cols[i].metric(
                    row['region'],
                    f"{row['renewable_percentage']:.1f}%",
                    delta=None
                )
            
            # Display data table with better styling
            st.header("Regional Data")
            st.dataframe(
                df[['region', 'renewable_percentage', 'intensity_forecast', 'intensity_index']].sort_values('renewable_percentage', ascending=False).style.format({
                    "renewable_percentage": "{:.1f}%",
                    "intensity_forecast": "{} gCO2/kWh"
                }),
                use_container_width=True,
                height=500
            )
        else:
            st.info("No regional data available")
    except Exception as e:
        st.error(f"Error fetching regional data: {str(e)}")