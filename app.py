from dash import dash_table, dash, html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import seaborn as sb
import pandas as pd
from dash import Dash, dcc, html, Input, Output, callback_context
import os
import warnings
import calendar
from collections import Counter
import threading
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# Global variables with thread safety
data_lock = threading.Lock()
last_refresh_time = None
REFRESH_INTERVAL = 300  # 5 minutes in seconds

def load_all_data():
    """Load all data from source with thread safety"""
    global df_meter, df_cost, df_cost_2025, df_downTime, run_time, df_agg
    global df_supplied, df_stock, df_rc, df_rc_melt, power_df, last_refresh_time
    
    with data_lock:
        try:
            # Check if we need to refresh (5-minute interval)
            current_time = datetime.now()
            if (last_refresh_time is None or 
                (current_time - last_refresh_time).total_seconds() >= REFRESH_INTERVAL):
                
                print("Refreshing data from source...")
                
                url = "https://docs.google.com/spreadsheets/d/1O-mPctFgp6oqd-VK9YKHyPq-asuve2ZM/export?format=xlsx"
                df = pd.ExcelFile(url)

                # Meter data
                df_meter = df.parse(0)
                month_order = ["January","February","March","April","May","June","July","August","September","October","November","December"]
                df_meter['Month'] = pd.Categorical(df_meter['Month'], categories=month_order, ordered=True)

                ## Cost Break_Down
                df_cost = df.parse(1)
                # df_cost.at[125, 'Rate'] = 127000
                # df_cost.at[125, 'Amount (NGN)'] = 127000
                # df_cost.at[126, 'Rate'] = 127000
                # df_cost.at[126, 'Amount (NGN)'] = 127000

                df_cost['Generator'].replace(['new 80kva', 'both 80kva', 'old 80kva', 'new 200kva', '55Kva'],
                                         ['80kva', '80kva', '80kva',  '200kva', '55kva' ], inplace= True)

                df_cost['Date'] = pd.to_datetime(df_cost['Date'])
                df_cost['Year'] = df_cost['Date'].dt.strftime('%Y')
                df_cost['Month'] = df_cost['Date'].dt.strftime('%B')

                corr_Rout = df_cost.loc[df_cost['Type of Activity'].isin(['Corrective maintenance', 'Routine Maintenance', 'Fuel'])].copy()
                corr_Rout.drop(columns=['id'], inplace=True, errors='ignore')
                corr_Rout.reset_index(drop= True, inplace=True)

                df_cost_2025 = corr_Rout.loc[corr_Rout['Year'] == '2025'].copy()

                ## Downtime
                df_downTime = df.parse(2)
                df_downTime = df_downTime.sort_values(by='Duration_Hours', ascending=False)
                df_downTime['Generator'] = df_downTime['Generator'].replace('88kva', '80kva')

                # Group downtime by month and generator
                df_downTime["Month"] = pd.Categorical(
                    df_downTime["Month"],
                    categories=month_order,
                    ordered=True
                )
                df_downTime = df_downTime.groupby(["Month", "Generator"], as_index=False)["Duration_Hours"].sum()

                ## Runtime
                run_time = df.parse(4)
                run_time['Date'] = pd.to_datetime(run_time['Date'])
                run_time['Month'] = run_time['Date'].dt.strftime('%B')
                run_time['Day'] = run_time['Date'].dt.strftime('%A')
                run_time['Generator'].replace(['20KVA', '200KVA', '80KVA', '55KVA'], ['20kva', '200kva', '80kva', '55kva'], inplace = True)
                df_agg = run_time.groupby(['Month', 'Generator'], as_index=False)['Hours Operated'].sum()
                df_agg['Month'] = pd.Categorical(df_agg['Month'], categories=month_order, ordered=True)
                df_agg = df_agg.sort_values(by='Month')

                # Fuel Supplied
                df_supplied = df.parse(3)
                df_supplied.drop(index=10, inplace=True, errors='ignore')

                df_supplied['Date'] = pd.to_datetime(df_supplied['Date'])
                df_supplied['Month'] = df_supplied['Date'].dt.strftime('%B')

                
                ## Stock
                df_stock = df.parse(5)
                df_stock['Total Available Stock'] = df_stock['Opening_Stock'] + df_stock['Purchased_Stock']
                df_stock.rename(columns={"Month": "Date"}, inplace=True)
                df_stock['Month'] = pd.to_datetime(df_stock['Date']).dt.strftime('%B')

                # pick the last record per Month, Generator_Size and Filter_Type
                df_rc = df_stock.sort_values('Date').groupby(
                    ['Month', 'Generator_Size', 'Filter_Type'], as_index=False
                ).last()[['Month', 'Generator_Size','Filter_Type','Consumed_Stock','Remaining_Stock']]

                # Melt for stacked bar plotting (Month included)
                df_rc_melt = df_rc.melt(
                    id_vars=['Month','Generator_Size','Filter_Type'],  
                    value_vars=['Consumed_Stock','Remaining_Stock'],
                    var_name='Stock_Status',
                    value_name='Units'
                )



                ## Power Transaction
                # path2 = "https://docs.google.com/spreadsheets/d/1O-mPctFgp6oqd-VK9YKHyPq-asuve2ZM/export?format=xlsx"
                # df2 = pd.ExcelFile(path2)
                power_df = df.parse(6)

                # Convert Transaction Date to string first to handle mixed types, then to datetime
                try:
                    power_df['Transaction Date'] = power_df['Transaction Date'].astype(str)
                    power_df['Transaction Date'] = pd.to_datetime(power_df['Transaction Date'], errors='coerce')
                    # Drop rows where date conversion failed
                    power_df = power_df.dropna(subset=['Transaction Date'])
                except Exception:
                    pass

                # Extract month name for easier filtering in callback (keep all months)
                if 'Transaction Date' in power_df.columns:
                    power_df['Month'] = power_df['Transaction Date'].dt.strftime('%B')

                power_df.reset_index(drop=True, inplace=True)
                
                last_refresh_time = current_time
                print("Data refresh completed successfully")
                
        except Exception as e:
            print(f"Error refreshing data: {e}")
            # Keep existing data if refresh fails

# Initial data load
load_all_data()

#======interactivity========

# Define all location/address options for filtering
all_locations = sorted(list(set(
    list(df_meter["Location"].unique()) +
    ['Rosewood', 'Cedar A', 'Tuck-shop', 'Cedar B',
     'Head Office', 'Engineering Yard', 'NBIC 2', 'NBIC 1',
     'HELIUM', 'DIC']
)))

# Location filter (now includes both meter locations and transaction addresses)
metr_loc = dcc.Dropdown(
        id='location_filter',
        options=[{"label": loc, "value": loc} for loc in all_locations],
        placeholder="Select Location",
        multi=True,
        style={'width': '90%', 'marginTop': '30%', "marginLeft": "5%"}
    )

# Month filter
mtr_month = dcc.Dropdown(
        id='month_filter',
        options=[{"label": m, "value": m} for m in run_time["Month"].unique()],
        placeholder="Select Month",
        multi=True,
        style={'width': '90%', 'marginTop': '30%', "marginLeft": "5%"}
    )

# Generator dropdown
# safe generator options (drop missing values and sort by string)
gens = run_time['Generator'].dropna().astype(str).unique().tolist()
gens = sorted(gens, key=lambda x: x.lower())  # case-insensitive sort

gen_dropdown = dcc.Dropdown(
    id='generator_type',
    options=[{"label": gen, "value": gen} for gen in gens],
    placeholder="Select Generator Type",
    multi=True,
    style={'width': '90%', 'marginTop': '30%', "marginLeft": "5%"}
)


# Graph components
consChart = dcc.Graph(id='consumption_chart', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"}, className='consumption-chart')

consumpLine = dcc.Graph(id='consumption_line', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"}, className='consumption-line')

costPie = dcc.Graph(id='cost_pie', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"}, className='cost-pie')

fuelChart = dcc.Graph(id='fuel_chart', className='fuel-chart', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"})

downtimeChart = dcc.Graph(id='downtime_chart', className='downtime-chart', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"})

stockChart = dcc.Graph(id='stock_chart', className='stock-chart', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"})

runtimeChart = dcc.Graph(id='runtime_chart', className='runtime-chart', config={"responsive": True},
    style={"width": "100%", "height": "100%", "flex": "1 1 auto"})


# Transactions table
transTable = dash_table.DataTable(
                id='transactions_table',
                columns=[{"name": "Meter Number", "id": "Meter Number"}],
                data=[],
                page_size=8,
                fixed_rows={'headers': True},
                style_table={'overflowX': 'auto', 'height': '90%', 'width': '98%'},
                style_header={
                    'backgroundColor': '#f7f9fc',
                    'fontWeight': '20',
                    'borderBottom': '1px solid #ddd',
                    'fontSize': '12px'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '4px',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'minWidth': '25px',
                    'maxWidth': '150px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis'
                },
                css=[
                    {
                        'selector': '.dash-table-container .dash-spreadsheet-container',
                        'rule': 'height: calc(100% - 36px) !important;'
                    }
                ]
            )

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    html.Meta(name='viewport', content='width=device-width, initial-scale=1.0'),
    html.Div([
        html.Img(
            src=app.get_asset_url('images/Gracefield_logo.png'),
            className="logo",
            alt="Gracefield logo"
        ),
        mtr_month,
        metr_loc, 
        gen_dropdown,
        html.Button("Power Analytics", id="tab1-btn", className="tab-btn active-tab", 
                   style={"marginLeft": "0.5rem", "marginTop": "4rem"}),
        html.Button("Operations", id="tab2-btn", className="tab-btn",
                   style={"marginLeft": "0.8rem", "marginTop": "4rem"})
    ], className="sidebar"),
    
    html.Div([
        html.H2("Power Dashboard", className="title"),
        html.Div([
            html.Div("💼", className="kpi-icon"),
            html.Div([
                html.P("Gravitas Revenue", className="kpi-label"),
                html.H3(id="gravitas_revenue", className="kpi-value")
            ], className="kpi-text")
        ], className="kpi-card"),

        html.Div([
            html.Div("👥", className="kpi-icon"),
            html.Div([
                html.P("Subscriber Revenue", className="kpi-label"),
                html.H3(id="subs_revenue", className="kpi-value")
            ], className="kpi-text")
        ], className="kpi-card"),

        html.Div([
            html.Div("⏱️", className="kpi-icon"),
            html.Div([
                html.P("Operated Hours", className="kpi-label"),
                html.H3(id="operated_hours", className="kpi-value")
            ], className="kpi-text")
        ], className="kpi-card"),

        html.Div([
            html.Div("⏸️", className="kpi-icon"),
            html.Div([
                html.P("Planned Outage", className="kpi-label"),
                html.H3(id="planned_outage", className="kpi-value")
            ], className="kpi-text")
        ], className="kpi-card")
    ], className="header"),

    html.Div([
        html.Div([
            consumpLine
        ], className="card-1"),

        html.Div([
            consChart
        ], className="card-2"),

        html.Div([
            transTable
        ], className="card-3"),

        html.Div([
            costPie
        ], className="card-4"),    
    ], id="tab-1", className="section"),

    html.Div([
        html.Div([
            fuelChart
        ], className="card-1"),

        html.Div([
            downtimeChart
        ], className="card-2"),

        html.Div([
            stockChart
        ], className="card-3"),

        html.Div([
            runtimeChart
        ], className="card-4"),    
    ], id="tab-2", className="section", style={"display": "none"}),
    
    dcc.Interval(id='data-refresh-interval', interval=300000, n_intervals=0),  # 5 minutes = 300000 ms
], className="app-grid")

# Tab switching callback
@app.callback(
    [
        Output('tab-1', 'style'),
        Output('tab-2', 'style'),
        Output('tab1-btn', 'className'),
        Output('tab2-btn', 'className'),
    ],
    [
        Input('tab1-btn', 'n_clicks'),
        Input('tab2-btn', 'n_clicks'),
    ],
    prevent_initial_call=False
)
def switch_tabs(tab1_clicks, tab2_clicks):
    ctx = callback_context
    if not ctx.triggered:
        # Initial load: show tab-1
        return {'display': 'flex'}, {'display': 'none'}, 'tab-btn active-tab', 'tab-btn'
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'tab1-btn':
        return {'display': 'flex'}, {'display': 'none'}, 'tab-btn active-tab', 'tab-btn'
    else:
        return {'display': 'none'}, {'display': 'flex'}, 'tab-btn', 'tab-btn active-tab'

@app.callback(
    [
        Output('consumption_chart', 'figure'),
        Output('consumption_line', 'figure'),
        Output('transactions_table', 'data'),
        Output('transactions_table', 'columns'),
        Output('gravitas_revenue', 'children'),
        Output('subs_revenue', 'children'),
        Output('operated_hours', 'children'),
        Output('planned_outage', 'children'),
        Output('cost_pie', 'figure'),
        Output('fuel_chart', 'figure'),
        Output('downtime_chart', 'figure'),
        Output('stock_chart', 'figure'),
        Output('runtime_chart', 'figure'),
    ],
    [
        Input('location_filter', 'value'),
        Input('month_filter', 'value'),
        Input('generator_type', 'value'),
        Input('data-refresh-interval', 'n_intervals'),
    ]
)
def update_chart(selected_locations, selected_months, selected_generators, n_intervals):
    # Load/refresh data with thread safety
    load_all_data()
    
    # Create thread-safe copies of the data for this callback
    with data_lock:
        local_df_meter = df_meter.copy()
        local_df_cost_2025 = df_cost_2025.copy()
        local_power_df = power_df.copy()
        local_df_supplied = df_supplied.copy()
        local_df_downTime = df_downTime.copy()
        local_df_rc_melt = df_rc_melt.copy()
        local_df_agg = df_agg.copy()

    filtered_meter = local_df_meter.copy()

    if selected_locations:
        filtered_meter = filtered_meter[filtered_meter["Location"].isin(selected_locations)]

    if selected_months:
        filtered_meter = filtered_meter[filtered_meter["Month"].isin(selected_months)]

    filtered_meter['Rate'] = filtered_meter['Location'].apply(lambda x: 285 if x in ['9mobile', 'Providus'] else 100)
    filtered_meter['Amount'] = filtered_meter['Rate'] * filtered_meter['Monthly_Consumption']
    
    gravitas_partner = round(filtered_meter.loc[
        filtered_meter['Location'].isin(['9mobile', 'Providus']), "Amount"
    ].sum(), 2)

    gravitas_subscriber = round(filtered_meter.loc[
        filtered_meter['Location'] == 'Canteen', "Amount"
    ].sum(), 2)
    
    # --- Bar chart with brand-aligned color palette ---
    brand_colors = ['#C7A64F', '#2C3E50', "#5E7286", '#F4E4C1', '#E8D5B7']
    
    meter_grouped = filtered_meter.groupby('Location', as_index=False)['Monthly_Consumption'].sum()
    fig_bar = px.bar(
        meter_grouped,
        x='Location',
        y='Monthly_Consumption',
        color='Location',
        text_auto=False,
        labels={'Monthly_Consumption': 'Consumption'},
        color_discrete_sequence=brand_colors
    )

    fig_bar.update_layout(
        title=dict(text='⚡ Power Consumption by Location', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        showlegend=False,
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=20, r=20)
    )

    # --- Line chart with brand colors ---
    fig_line = px.line(
        filtered_meter,
        x='Month',
        y='Amount',
        color='Location',
        markers=True,
        color_discrete_sequence=brand_colors
    )

    fig_line.update_layout(
        title=dict(text='⚡ Monthly Consumption Amount Trend', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        xaxis_title='Month',
        yaxis_title='Amount',
        template="plotly_white",
        #showlegend=False,
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=20, r=20)
    )
    
    # Style the line traces with gold tones
    fig_line.update_traces(line=dict(width=2.5))

    # --- Transactions table ---
    table_df = local_power_df.copy()

    # Filter by month
    if selected_months:
        months_selected = selected_months if isinstance(selected_months, list) else [selected_months]
        table_df = table_df[table_df['Month'].isin(months_selected)]

    # Fix addresses to match the corrected names
    table_df.loc[table_df['Meter Number'] == 23220035788, "Resident Address"] = 'Rosewood'
    table_df.loc[table_df['Meter Number'] == 4293682789, "Resident Address"] = 'NBIC 2' 

    table_df['Resident Address'] = table_df['Resident Address'].replace(['C A','Bites To Eat [Tuck-shop]',
                                                'Gravitas Head Office', 'Gravitas Engineering Yard', 'HELIUM '],
                                                ['Cedar A', 'Tuck-shop', 'Head Office', 'Engineering Yard', 'HELIUM'])
    

    mask = (table_df['Resident Address'] == 'Cedar A') & (table_df['Meter Number'] == 4293684496)
    if not table_df.loc[mask].empty:
        min_index = table_df.loc[mask, 'Amount'].idxmin()
        table_df.loc[min_index, 'Resident Address'] = 'Cedar B'

    

    # Filter by selected location/address
    if selected_locations:
        locations_selected = selected_locations if isinstance(selected_locations, list) else [selected_locations]
        table_df = table_df[table_df['Resident Address'].isin(locations_selected)]

    if not table_df.empty:
        # Use the same approach as your Jupyter notebook
        pivot_list = []
        for col in table_df['Resident Address'].unique():
            temp = pd.pivot_table(
                table_df[table_df["Resident Address"] == col],
                values="Amount",
                index="Meter Number",
                columns="Resident Address",
                aggfunc="sum"
            )
            pivot_list.append(temp)
        
        pivot = pd.concat(pivot_list, axis=0).fillna('-').reset_index()
    else:
        pivot = pd.DataFrame(columns=['Meter Number'])

    # ---- ROUND TO 2 SIGNIFICANT FIGURES ----
    cols_to_format = [col for col in pivot.columns if col != "Meter Number"]

    pivot[cols_to_format] = pivot[cols_to_format].applymap(
        lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x
    )

    # pivot.rename(columns={
    #     'C A': 'Cedar A',
    #     'Gravitas Engineering Yard': 'Engineering Yard',
    #     'Gravitas Head Office': 'Head Office',
    #     'Bites To Eat [Tuck-shop]': 'Tuck-shop'
    # }, inplace=True)

    # print(pivot.columns)

    table_data = pivot.to_dict('records')
    table_columns = [{"name": str(col), "id": str(col)} for col in pivot.columns]

    df_table = pd.DataFrame(table_data)

    # --- Gravitas Partner Revenue ---
    def safe_sum(col):
        if col in df_table.columns:
            return pd.to_numeric(df_table[col].replace('-', 0), errors='coerce').sum()
        return 0

    gho = safe_sum("Head Office")
    gey = safe_sum("Engineering Yard")

    total_gravitas = gho + gey + gravitas_partner
    gravitas_revenue = f"₦{total_gravitas:,.0f}"

    # --- Subscriber Revenue ---
    df_table.columns = df_table.columns.str.strip().str.replace('\u00A0', '', regex=True)

    columns_to_sum = ['Cedar A', 'DIC', 'NBIC 1', 'NBIC 2', 'HELIUM', 
                    'Rosewood', 'Tuck-shop', 'Cedar B']

    existing_cols = [c for c in columns_to_sum if c in df_table.columns]

    subs_sum = (
        df_table[existing_cols]
            .replace('-', 0)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0)
            .to_numpy()
            .sum()
    )

    total_subs = subs_sum + gravitas_subscriber
    gravitas_subs_revenue = f"₦{total_subs:,.0f}"   

    # --- Cost Pie Chart ---
    filtered_cost = local_df_cost_2025.copy() 
    
    if selected_generators:
        filtered_cost = filtered_cost[filtered_cost["Generator"].isin(selected_generators)]
    
    if selected_months:
        filtered_cost = filtered_cost[filtered_cost["Month"].isin(selected_months)]
    
    fig_pie = px.pie(
        filtered_cost,
        names='Type of Activity',
        values='Amount (NGN)',
        color_discrete_sequence=brand_colors,
    )   

    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
    fig_pie.update_layout(
        title=dict(text='💸 Cost Breakdown (2025)', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=8, r=8)
    )

    # --- Fuel Chart (Tab-2) ---
    filtered_fuel = local_df_supplied.copy()
    if selected_months:
        filtered_fuel = filtered_fuel[filtered_fuel['Month'].isin(selected_months)]
    
    filtered_fuel['Total Fuel Used'] = pd.to_numeric(filtered_fuel['Total Fuel Used'], errors='coerce')
    filtered_fuel['Fuel Added (Total)'] = pd.to_numeric(filtered_fuel['Fuel Added (Total)'], errors='coerce')
    filtered_fuel = filtered_fuel.dropna(subset=['Total Fuel Used', 'Fuel Added (Total)'])
    
    if not filtered_fuel.empty:
        fig_fuel = px.bar(
            filtered_fuel,
            x='Month',
            y=['Total Fuel Used', 'Fuel Added (Total)'],
            barmode='group',
            labels={'Month': 'Month', 'value': 'Fuel (Litres)'},
            color_discrete_sequence=['#C7A64F', '#2C3E50']
        )
    else:
        fig_fuel = px.bar(title="No fuel data available")
    
    fig_fuel.update_layout(
        title=dict(text='⛽ Fuel Management', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=20, r=120),
        legend=dict(
            orientation='v',
            x=1.02,
            xanchor='left',
            y=1,
            yanchor='top',
            font=dict(size=10),
            bgcolor='rgba(0,0,0,0)',
            borderwidth=0
        )
    )

    # --- Downtime Chart (Tab-2) ---
    filtered_downtime = local_df_downTime.copy()

    if selected_months:
        filtered_downtime = filtered_downtime[filtered_downtime['Month'].isin(selected_months)]

    if selected_generators:
        filtered_downtime = filtered_downtime[filtered_downtime['Generator'].isin(selected_generators)]
    
    fig_down = px.bar(
        filtered_downtime,
        x="Month",
        y="Duration_Hours",
        color="Generator",
        text_auto=True,
        barmode="group",
        color_discrete_sequence=brand_colors,
    )

    fig_down.update_yaxes(type="log")

    fig_down.update_layout(
        title=dict(text='🛠️ Generator Downtime', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        xaxis_title="Month",
        template="plotly_white",
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=40, l=40, r=160),
        legend=dict(
            orientation='v',
            x=1.03,
            xanchor='left',
            y=1,
            yanchor='top',
            font=dict(size=10),
            bgcolor='rgba(0,0,0,0)',
            borderwidth=0
        )
    )

   
    # --- Stock Chart (Tab-2) with brand colors ---
    if selected_months:
        local_df_rc_melt = local_df_rc_melt[local_df_rc_melt['Month'].isin(selected_months)]

    if selected_generators:
        local_df_rc_melt = local_df_rc_melt[local_df_rc_melt['Generator_Size'].isin(selected_generators)]
       
    if not local_df_rc_melt.empty:
        fig_stock = px.bar(
            local_df_rc_melt,
            x='Filter_Type',
            y='Units',
            color='Stock_Status',
            barmode='stack',
            labels={'Units': 'Units', 'Filter_Type': 'Filter Type'},
            color_discrete_sequence=['#C7A64F', '#34495E']
        )
    else:
        fig_stock = px.bar(title="No stock data available")
    
    fig_stock.update_layout(
        title=dict(text='📦 Stock Inventory', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=20, r=120),
        legend=dict(
            orientation='v',
            x=1.02,
            xanchor='left',
            y=1,
            yanchor='top',
            font=dict(size=10),
            bgcolor='rgba(0,0,0,0)',
            borderwidth=0
        )
    )


    # --- Runtime Chart (Tab-2) ---
    filtered_runtime = local_df_agg.copy()

    if selected_months:
        filtered_runtime = filtered_runtime[filtered_runtime['Month'].isin(selected_months)]

    if selected_generators:
        filtered_runtime = filtered_runtime[filtered_runtime['Generator'].isin(selected_generators)]
    
    # Check if data exists after filtering
    if not filtered_runtime.empty:
        fig_runtime = px.pie(
            filtered_runtime,
            names="Generator",
            values="Hours Operated",
            color_discrete_sequence=brand_colors,
            hole=0.4
        )
    else:
        # Create empty pie chart if no data
        fig_runtime = px.pie(
            names=["No Data"],
            values=[1],
            color_discrete_sequence=brand_colors,
            hole=0.4
        )

    # Show percent + label inside slices
    fig_runtime.update_traces(textposition="inside", textinfo="percent+label")


    fig_runtime.update_layout(
        title=dict(text='⏱️ Generator Runtime', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=8, r=8)
    )

    # === Operated Hours + Planned Outage Calculation ===
    filtered_runtime = local_df_agg.copy()
    if selected_months:
        filtered_runtime = filtered_runtime[filtered_runtime['Month'].isin(selected_months)]
    if selected_generators:
        filtered_runtime = filtered_runtime[filtered_runtime['Generator'].isin(selected_generators)]

    # Get actual operated hours from the data
    actual_operated_hours = filtered_runtime['Hours Operated'].sum()

    # Define daily generator schedule based on the day of week
    # Weekday mapping: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
    daily_schedule = {
        0: {  # Monday
            "80kva": 11,      # 7AM - 6PM
            "55kva": 12,      # 7PM Monday - 7AM Tuesday
        },
        1: {  # Tuesday
            "80kva": 11,      # 7AM - 6PM
            "55kva": 12,      # 7PM Tuesday - 7AM Wednesday
        },
        2: {  # Wednesday
            "80kva": 11,      # 7AM - 6PM
            "55kva": 12,      # 7PM Wednesday - 7AM Thursday
        },
        3: {  # Thursday
            "80kva": 11,      # 7AM - 6PM
            "55kva": 12,      # 7PM Thursday - 7AM Friday
        },
        4: {  # Friday
            "80kva": 11,      # 7AM - 6PM
            "55kva": 19.5,    # 7PM Friday - 2:30PM Saturday
        },
        5: {  # Saturday
            "55kva": 12,      # 7PM Saturday - 7AM Sunday
        },
        6: {  # Sunday
            "200kva": 7,      # 7AM - 2PM
            "55kva": 12,      # 7PM Sunday - 7AM Monday
        }
    }

    # Determine the date range based on selected months
    if selected_months:
        months = selected_months if isinstance(selected_months, list) else [selected_months]
        # Convert month names to numbers
        month_nums = [list(calendar.month_name).index(m) for m in months]
        start_month = min(month_nums)
        end_month = max(month_nums)
        year = 2025
        
        # Get first day of first month and last day of last month
        start_date = datetime(year, start_month, 1)
        last_day = calendar.monthrange(year, end_month)[1]
        end_date = datetime(year, end_month, last_day)
    else:
        # If no month selected, use entire year
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 12, 31)

    # Generate all dates in the range and count each weekday occurrence
    date_range = pd.date_range(start_date, end_date, freq='D')
    weekday_counts = Counter(date_range.weekday)

    # Calculate total scheduled hours
    total_scheduled_hours = 0.0
    scheduled_breakdown = {}

    for weekday, count in weekday_counts.items():
        day_schedule = daily_schedule.get(weekday, {})
        
        for gen, hours_per_day in day_schedule.items():
            # Skip if this generator is not in the selected filter
            if selected_generators and gen not in selected_generators:
                continue
            
            # Calculate scheduled hours for this generator on this weekday
            scheduled_hours = count * hours_per_day
            total_scheduled_hours += scheduled_hours
            
            # Track breakdown for debugging
            day_name = calendar.day_name[weekday]
            key = f"{gen}_{day_name}"
            scheduled_breakdown[key] = scheduled_hours

    # Calculate total hours available in the period
    total_days = (end_date - start_date).days + 1
    total_hours_in_period = total_days * 24

    # Planned outage = Total hours - Operated hours
    planned_outage_hours = max(total_hours_in_period - actual_operated_hours, 0)

    # Format for display
    operated_hours_display = f"{actual_operated_hours:,.0f}h"
    planned_outage_display = f"{planned_outage_hours:,.0f}h"

    # Debug print (optional - detailed breakdown)
    # print(f"\n{'='*60}")
    # print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    # print(f"{'='*60}")
    # print(f"Weekday Distribution:")
    # for weekday in range(7):
    #     day_name = calendar.day_name[weekday]
    #     count = weekday_counts.get(weekday, 0)
    #     print(f"  {day_name:10s}: {count} days")

    # print(f"\nScheduled Hours Breakdown:")
    # for key, hours in sorted(scheduled_breakdown.items()):
    #     print(f"  {key:20s}: {hours:6.1f}h")

    # print(f"\n{'='*60}")
    # print(f"Total Days in Period    : {total_days}")
    # print(f"Total Hours Available   : {total_hours_in_period:,}h")
    # print(f"Total Scheduled Hours   : {total_scheduled_hours:,.1f}h")
    # print(f"Actual Operated Hours   : {actual_operated_hours:,.0f}h")
    # print(f"Planned Outage Hours    : {planned_outage_hours:,.0f}h")
    # print(f"{'='*60}\n")


    return (fig_bar, fig_line, table_data, table_columns, 
        gravitas_revenue, gravitas_subs_revenue, 
        operated_hours_display, planned_outage_display,
        fig_pie, fig_fuel, fig_down, fig_stock, fig_runtime)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    debug_mode = os.environ.get("DASH_DEBUG_MODE", "false").lower() == "true"
    
    app.run(
        debug=debug_mode,
        host="0.0.0.0",
        port=port,
        dev_tools_ui=debug_mode,
        dev_tools_props_check=debug_mode
    )