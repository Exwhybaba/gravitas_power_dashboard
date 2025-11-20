from dash import dash_table, dash, html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import seaborn as sb
import pandas as pd
from dash import Dash, dcc, html, Input, Output, callback_context
import os
import warnings
warnings.filterwarnings('ignore')



url = "https://docs.google.com/spreadsheets/d/1O-mPctFgp6oqd-VK9YKHyPq-asuve2ZM/export?format=xlsx"
df = pd.ExcelFile(url)


df_meter = df.parse(0)

month_order = ["January","February","March","April","May","June","July","August","September","October","November","December"]

df_meter['Month'] = pd.Categorical(df_meter['Month'], categories=month_order, ordered=True)



## Cost Break_Down

df_cost =  df.parse(1)
df_cost.at[125, 'Rate'] = 127000
df_cost.at[125, 'Amount (NGN)'] = 127000
df_cost.at[126, 'Rate'] = 127000
df_cost.at[126, 'Amount (NGN)'] = 127000

df_cost['Generator'].replace(['new 80kva', 'both 80kva', 'old 80kva', 'new 200kva', '55Kva'],
                             ['80kva', '80kva', '80kva',  '200kva', '55kva' ], inplace= True)

df_cost['Date'] = pd.to_datetime(df_cost['Date'])
df_cost['Year'] = df_cost['Date'].dt.strftime('%Y')
df_cost['Month'] = df_cost['Date'].dt.strftime('%B')

corr_Rout = df_cost.loc[df_cost['Type of Activity'].isin(['Corrective maintenance', 'Routine Maintenance', 'Fuel'])].copy()
corr_Rout.drop(columns=['id'], inplace=True)
corr_Rout.reset_index(drop= True, inplace=True)

#this would be change to include other years later
df_cost_2025 = corr_Rout.loc[corr_Rout['Year'] == '2025'].copy()




## Downtime
df_downTime = df.parse(2)
df_downTime = df_downTime.sort_values(by ='Duration_Hours', ascending= False)
df_downTime['Generator'] = df_downTime['Generator'].replace('88kva', '80kva')

# df_downTime = df_downTime.groupby(["Month", "Generator"], as_index=False)["Duration_Hours"].sum()

## runtime
run_time = df.parse(4)
run_time['Date'] = pd.to_datetime(run_time['Date'])

run_time['Month']=run_time['Date'].dt.strftime('%B')
run_time['Day'] = run_time['Date'].dt.strftime('%A')
df_agg = run_time.groupby(['Month', 'Generator'], as_index=False)['Hours Operated'].sum()
df_agg['Month'] = pd.Categorical(df_agg['Month'], categories=month_order, ordered=True)
df_agg = df_agg.sort_values(by='Month')



# Fuel Supplied
df_supplied = df.parse(3)
df_supplied.at[1, 'Total Fuel Used'] = 4200
df_supplied.at[1, 'Fuel Added (Total)'] = 3000
df_supplied.drop(index = 10, inplace = True)

df_supplied['Date'] = pd.to_datetime(df_supplied['Date'])
df_supplied['Month'] = df_supplied['Date'].dt.strftime('%B')




##stock
df_stock = df.parse(5)
df_stock['Total Available Stock'] = df_stock['Opening_Stock'] + df_stock['Purchased_Stock']

df_stock.rename(columns={"Month": "Date"}, inplace = True)
df_stock['Month'] = pd.to_datetime(df_stock['Date']).dt.strftime('%B')

# Aggregate by Generator_Size + Filter_Type
df_rc = df_stock.groupby(['Generator_Size','Filter_Type'], as_index=False).agg({
    'Consumed_Stock':'sum',
    'Remaining_Stock':'sum'
})

# Melt for stacked plotting
df_rc_melt = df_rc.melt(
    id_vars=['Generator_Size','Filter_Type'],
    value_vars=['Consumed_Stock','Remaining_Stock'],
    var_name='Stock_Status',
    value_name='Units'
)


def refresh_data():
    """Reload data files from disk (or URL) and update module-level dataframes.
    Safe to call repeatedly; logs failures silently and keeps previous data if reload fails.
    """
    global df, df_meter, df_cost, df_cost_2025, df_downTime, run_time, df_agg
    global df_supplied, df_stock, df_rc, df_rc_melt, power_df
    try:
        # reload the Excel source (same URL/path used above)
        df_new = pd.ExcelFile(url)

        df_meter_new = df_new.parse(0)
        df_meter_new['Month'] = pd.Categorical(df_meter_new['Month'], categories=month_order, ordered=True)

        # cost sheet
        df_cost_new = df_new.parse(1)
        try:
            df_cost_new.at[125, 'Rate'] = 127000
            df_cost_new.at[125, 'Amount (NGN)'] = 127000
            df_cost_new.at[126, 'Rate'] = 127000
            df_cost_new.at[126, 'Amount (NGN)'] = 127000
        except Exception:
            pass
        df_cost_new['Generator'].replace(['new 80kva', 'both 80kva', 'old 80kva', 'new 200kva', '55Kva'],
                                         ['80kva', '80kva', '80kva',  '200kva', '55kva' ], inplace= True)
        df_cost_new['Date'] = pd.to_datetime(df_cost_new['Date'])
        df_cost_new['Year'] = df_cost_new['Date'].dt.strftime('%Y')
        df_cost_new['Month'] = df_cost_new['Date'].dt.strftime('%B')
        corr_Rout_new = df_cost_new.loc[df_cost_new['Type of Activity'].isin(['Corrective maintenance', 'Routine Maintenance', 'Fuel'])]
        try:
            corr_Rout_new.drop(columns=['id'], inplace=True)
        except Exception:
            pass
        df_cost_2025_new = corr_Rout_new.loc[corr_Rout_new['Year'] == '2025']

        # downtime
        df_downTime_new = df_new.parse(2)
        df_downTime_new = df_downTime_new.sort_values(by ='Duration_Hours', ascending= False)
        df_downTime_new['Generator'] = df_downTime_new['Generator'].replace('88kva', '80kva')

        month_order = [
            "January","February","March","April","May","June",
            "July","August","September","October","November","December"
        ]

        df_downTime_new["Month"] = pd.Categorical(
            df_downTime_new["Month"],
            categories=month_order,
            ordered=True
        )

        df_downTime_new = df_downTime_new.groupby(["Month", "Generator"], as_index=False)["Duration_Hours"].sum()

        # runtime
        run_time_new = df_new.parse(4)
        run_time_new['Date'] = pd.to_datetime(run_time_new['Date'])
        run_time_new['Month']=run_time_new['Date'].dt.strftime('%B')
        run_time_new['Day'] = run_time_new['Date'].dt.strftime('%A')
        df_agg_new = run_time_new.groupby(['Month', 'Generator'], as_index=False)['Hours Operated'].sum()
        df_agg_new['Month'] = pd.Categorical(df_agg_new['Month'], categories=month_order, ordered=True)
        df_agg_new = df_agg_new.sort_values(by='Month')

        # fuel supplied
        df_supplied_new = df_new.parse(3)
        try:
            df_supplied_new.at[1, 'Total Fuel Used'] = 4200
            df_supplied_new.at[1, 'Fuel Added (Total)'] = 3000
            df_supplied_new.drop(index = 10, inplace = True)
        except Exception:
            pass
        df_supplied_new['Date'] = pd.to_datetime(df_supplied_new['Date'])
        df_supplied_new['Month'] = df_supplied_new['Date'].dt.strftime('%B')

        # stock
        df_stock_new = df_new.parse(5)
        df_stock_new['Total Available Stock'] = df_stock_new['Opening_Stock'] + df_stock_new['Purchased_Stock']
        df_stock_new.rename(columns={"Month": "Date"}, inplace = True)
        df_stock_new['Month'] = pd.to_datetime(df_stock_new['Date']).dt.strftime('%B')
        df_rc_new = df_stock_new.groupby(['Generator_Size','Filter_Type'], as_index=False).agg({
            'Consumed_Stock':'sum',
            'Remaining_Stock':'sum'
        })
        df_rc_melt_new = df_rc_new.melt(
            id_vars=['Generator_Size','Filter_Type'],
            value_vars=['Consumed_Stock','Remaining_Stock'],
            var_name='Stock_Status',
            value_name='Units'
        )

        # If all of the above parsing succeeded, swap in the new dataframes
        df = df_new
        df_meter = df_meter_new
        df_cost = df_cost_new
        df_cost_2025 = df_cost_2025_new
        df_downTime = df_downTime_new
        run_time = run_time_new
        df_agg = df_agg_new
        df_supplied = df_supplied_new
        df_stock = df_stock_new
        df_rc = df_rc_new
        df_rc_melt = df_rc_melt_new

    except Exception:
        # keep existing data on failure
        return

    # reload power transactions (Excel sheet 6) if available
    try:
        df2_new = pd.ExcelFile(path2)
        power_df_new = df2_new.parse(6)
        power_df_new['Transaction Date'] = power_df_new['Transaction Date'].astype(str)
        power_df_new['Transaction Date'] = pd.to_datetime(power_df_new['Transaction Date'], errors='coerce')
        power_df_new = power_df_new.dropna(subset=['Transaction Date'])
        if 'Transaction Date' in power_df_new.columns:
            power_df_new['Month'] = power_df_new['Transaction Date'].dt.strftime('%B')
        power_df_new.reset_index(drop=True, inplace=True)
        power_df = power_df_new
    except Exception:
        pass





##Power Transaction
path2 = "https://docs.google.com/spreadsheets/d/1O-mPctFgp6oqd-VK9YKHyPq-asuve2ZM/export?format=xlsx"
df2 = pd.ExcelFile(path2)
power_df = df2.parse(6)

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




#======interactivity========

# Define all location/address options for filtering
all_locations = sorted(list(set(
    list(df_meter["Location"].unique()) +
    ['Rosewood', 'Cedar A', 'Tuck-shop', 'Cedar B',
     'Gravitas Head Office', 'Engineering Yard', 'NBIC 2', 'NBIC 1',
     'HELIUM ', 'DIC']
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
        options=[{"label": m, "value": m} for m in df_meter["Month"].unique()],
        placeholder="Select Month",
        multi=True,
        style={'width': '90%', 'marginTop': '30%', "marginLeft": "5%"}
    )

# Generator dropdown
gen_dropdown = dcc.Dropdown(
        id='generator_type',
        options=[{"label": gen, "value": gen} for gen in sorted(df_cost_2025["Generator"].unique())],
        placeholder="Select Generator Type",
        multi=True,
        style={'width': '90%', 'marginTop': '30%', "marginLeft": "5%"}
        )

# Graph
consChart = dcc.Graph(id='consumption_chart', className='consumption-chart')
consumpLine = dcc.Graph(id='consumption_line', className='consumption-line')
costPie = dcc.Graph(id='cost_pie', className='cost-pie')


fuelChart = dcc.Graph(id='fuel_chart', className='fuel-chart')
downtimeChart = dcc.Graph(id='downtime_chart', className='downtime-chart')
stockChart = dcc.Graph(id='stock_chart', className='stock-chart')
runtimeChart = dcc.Graph(id='runtime_chart', className='runtime-chart')


# (transactions table markup will be placed directly inside card-3 in the layout)
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
         # Add toggle button as a Dash component instead of pure JavaScript
            # html.Button(
            #     "☰",
            #     id="sidebar-toggle-btn",
            #     className="sidebar-toggle-js",
            #     style={"display": "none"}  # Hidden by default, shown via CSS
            #     ),

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

    # html.Div([
    #     html.Span("© 2025 Gracefield. All rights reserved. "),
    # ], className="footer")  

    
    dcc.Interval(id='data-refresh-interval', interval=60000, n_intervals=0),
], className= "app-grid")




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
    # If the interval ticked, attempt to refresh the source data first.
    try:
        refresh_data()
    except Exception:
        pass

    filtered_meter = df_meter.copy()

    if selected_locations:
        filtered_meter = filtered_meter[filtered_meter["Location"].isin(selected_locations)]

    if selected_months:
        filtered_meter = filtered_meter[filtered_meter["Month"].isin(selected_months)]

    filtered_meter['Rate'] = filtered_meter['Location'].apply(lambda x: 285 if x in ['9mobile', 'Providus'] else 100)
    filtered_meter['Amount'] = filtered_meter['Rate'] * filtered_meter['Monthly_Consumption']
    
    gravitas_partner = filtered_meter.loc[
        filtered_meter['Location'].isin(['9mobile', 'Providus']), "Amount"
    ].sum()

    gravitas_subscriber = filtered_meter.loc[
        filtered_meter['Location'] == 'Canteen', "Amount"
    ].sum()
    
    # --- Bar chart with brand-aligned color palette ---
    # Custom color palette: Gold, Navy, Slate, Cream
    brand_colors = ['#C7A64F', '#2C3E50', '#34495E', '#F4E4C1', '#E8D5B7']
    
    fig_bar = px.bar(
        filtered_meter,
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
        height=210,
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
        showlegend=False,
        width=488,
        height=200,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=20, r=20)
    )
    
    # Style the line traces with gold tones
    fig_line.update_traces(line=dict(width=2.5))

    # --- Transactions table ---
    table_df = power_df.copy()

    # Filter by month
    if selected_months:
        months_selected = selected_months if isinstance(selected_months, list) else [selected_months]
        table_df = table_df[table_df['Month'].isin(months_selected)]

    # Fix addresses to match the corrected names
    table_df.loc[table_df['Meter Number'] == 23220035788, "Resident Address"] = 'Rosewood'
    table_df.loc[table_df['Meter Number'] == 4293682789, "Resident Address"] = 'NBIC 2' 

    mask = (table_df['Resident Address'] == 'C A') & (table_df['Meter Number'] == 4293684496)
    if not table_df.loc[mask].empty:
        min_index = table_df.loc[mask, 'Amount'].idxmin()
        table_df.loc[min_index, 'Resident Address'] = 'Cedar B'

    # Filter by selected location/address (NEW)
    if selected_locations:
        locations_selected = selected_locations if isinstance(selected_locations, list) else [selected_locations]
        table_df = table_df[table_df['Resident Address'].isin(locations_selected)]

    if not table_df.empty:
        pivot = pd.pivot_table(
            table_df,
            values='Amount',
            index='Meter Number',
            columns='Resident Address',
            aggfunc='sum'
        ).fillna('-').reset_index()
    else:
        pivot = pd.DataFrame(columns=['Meter Number'])

    table_data = pivot.to_dict('records')
    table_columns = [{"name": str(col), "id": str(col)} for col in pivot.columns]

    df_table = pd.DataFrame(table_data)

    # --- Gravitas Partner Revenue ---
    def safe_sum(col):
        if col in df_table.columns:
            return pd.to_numeric(df_table[col].replace('-', 0), errors='coerce').sum()
        return 0

    gho = safe_sum("Gravitas Head Office")
    gey = safe_sum("Gravitas Engineering Yard")

    gravitas_revenue = gho + gey + gravitas_partner

    # --- Subscriber Revenue ---
    # Fix column names – remove trailing spaces and invisible unicode
    df_table.columns = df_table.columns.str.strip().str.replace('\u00A0', '', regex=True)

    # Normalize subscriber columns list
    columns_to_sum = ['C A', 'DIC', 'NBIC 1', 'NBIC 2', 'HELIUM', 
                    'Rosewood', 'Bites To Eat [Tuck-shop]', 'Cedar B']

    # Only sum columns that actually exist
    existing_cols = [c for c in columns_to_sum if c in df_table.columns]

    # Convert everything inside those columns to numeric safely
    subs_sum = (
        df_table[existing_cols]
            .replace('-', 0)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0)
            .to_numpy()
            .sum()
    )

    

    gravitas_subs_revenue = subs_sum + gravitas_subscriber

    # --- Cost Pie Chart ---
    filtered_cost = df_cost_2025.copy() 
    
    # Filter by generator type
    if selected_generators:
        filtered_cost = filtered_cost[filtered_cost["Generator"].isin(selected_generators)]
    
    # Filter by month  
    if selected_months:
        filtered_cost = filtered_cost[filtered_cost["Month"].isin(selected_months)]
    
    fig_pie = px.pie(
        filtered_cost,
        names='Type of Activity',
        values='Amount (NGN)',
        color_discrete_sequence=brand_colors,
        # title='Cost Breakdown by Activity Type (2025)'
    )   

    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
    fig_pie.update_layout(
        title=dict(text='💸 Cost Breakdown (2025)', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        height=210,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=8, r=8)
    )

    # --- Fuel Chart (Tab-2) ---
    filtered_fuel = df_supplied.copy()
    if selected_months:
        filtered_fuel = filtered_fuel[filtered_fuel['Month'].isin(selected_months)]
    
    # Convert to numeric to avoid type issues
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
        # Empty chart if no data
        fig_fuel = px.bar(title="No fuel data available")
    
    # Place legend to the right of the chart and make it smaller so it doesn't overlap bars
    fig_fuel.update_layout(
        title=dict(text='⛽ Fuel Management', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        height=210,
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
    filtered_downtime = df_downTime.copy()

    if selected_months:
        filtered_downtime = filtered_downtime[filtered_downtime['Month'].isin(selected_months)]
    
    
    fig_down = px.bar(
        filtered_downtime,
        x="Month",
        y="Duration_Hours",
        color="Generator",
        text_auto=True,
        barmode="group",
        color_discrete_sequence=brand_colors,
        #title="Downtime Duration by Month and Generator"
    )

    # Set y-axis to log scale
    fig_down.update_yaxes(type="log")

    # Format layout and move legend clear of the bars (right side)
    fig_down.update_layout(
        title=dict(text='🛠️ Generator Downtime', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        xaxis_title="Month",
        #yaxis_title="Total Duration (Hours, log scale)",
        template="plotly_white",
        width=500,
        height=210,
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
    if not df_rc_melt.empty:
        fig_stock = px.bar(
            df_rc_melt,
            x='Filter_Type',
            y='Units',
            color='Stock_Status',
            barmode='stack',
            labels={'Units': 'Units', 'Filter_Type': 'Filter Type'},
            color_discrete_sequence=['#C7A64F', '#34495E']
        )
    else:
        fig_stock = px.bar(title="No stock data available")
    
    # Move stock legend to the right and reduce its size to avoid overlap
    fig_stock.update_layout(
        title=dict(text='📦 Stock Inventory', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        height=210,
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
    filtered_runtime = df_agg.copy()

    if selected_months:
        filtered_runtime = filtered_runtime[filtered_runtime['Month'].isin(selected_months)]

    if selected_generators:
        filtered_runtime = filtered_runtime[filtered_runtime['Generator'].isin(selected_generators)]
    
    fig_runtime = px.pie(
        filtered_runtime,
        names="Generator",
        values="Hours Operated",
        color_discrete_sequence=brand_colors,
        hole=0.4
    )

    fig_runtime.update_traces(textposition="inside", textinfo="percent+label")

    # Apply same pie chart styling as Tab-1 cost_pie
    fig_runtime.update_layout(
        title=dict(text='⏱️ Generator Runtime', font=dict(size=12, color='#111827'), x=0.5, xanchor='center'),
        height=210,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=28, b=8, l=8, r=8)
    )

    # Calculate Operated Hours and Planned Outage
    operated_hours = filtered_runtime['Hours Operated'].sum()
    
    # Calculate total hours available in filtered months
    # 24 hours/day * number of days in each filtered month
    total_hours_available = 0
    if selected_months:
        filtered_months = selected_months if isinstance(selected_months, list) else [selected_months]
        # Days per month mapping
        days_in_month = {
            'January': 31, 'February': 28, 'March': 31, 'April': 30,
            'May': 31, 'June': 30, 'July': 31, 'August': 31,
            'September': 30, 'October': 31, 'November': 30, 'December': 31
        }
        for month in filtered_months:
            total_hours_available += days_in_month.get(month, 30) * 24
    else:
        # If no month filter, calculate for all months in data
        total_hours_available = len(df_agg['Month'].unique()) * 30 * 24
    
    # Planned Outage = Total Available Hours - Operated Hours
    planned_outage = max(0, total_hours_available - operated_hours)


    return fig_bar, fig_line, table_data, table_columns, gravitas_revenue, gravitas_subs_revenue, operated_hours, planned_outage, fig_pie, fig_fuel, fig_down, fig_stock, fig_runtime




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