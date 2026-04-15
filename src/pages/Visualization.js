import React, { useState } from 'react';

const visualizationScenarios = [
  {
    id: 1,
    category: 'Matplotlib',
    title: 'Bar Chart',
    desc: 'Create a grouped bar chart comparing categories',
    code: `import matplotlib.pyplot as plt
import pandas as pd

df = spark.sql("SELECT department, gender, AVG(salary) as avg_salary FROM employees GROUP BY department, gender").toPandas()

pivot_df = df.pivot(index='department', columns='gender', values='avg_salary')
ax = pivot_df.plot(kind='bar', figsize=(12, 6), width=0.8, color=['#2196F3', '#FF5722'])
ax.set_title('Average Salary by Department and Gender', fontsize=16, fontweight='bold')
ax.set_xlabel('Department', fontsize=12)
ax.set_ylabel('Average Salary ($)', fontsize=12)
ax.legend(title='Gender', fontsize=10)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
for container in ax.containers:
    ax.bar_label(container, fmt='$%.0f', padding=3, fontsize=8)
plt.tight_layout()
plt.savefig('/dbfs/tmp/bar_chart.png', dpi=150)
display(plt.gcf())`,
  },
  {
    id: 2,
    category: 'Matplotlib',
    title: 'Line Chart with Trends',
    desc: 'Plot time series data with moving average trend line',
    code: `import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

df = spark.sql("""
    SELECT date, SUM(revenue) as daily_revenue
    FROM sales
    GROUP BY date ORDER BY date
""").toPandas()
df['date'] = pd.to_datetime(df['date'])
df['ma_7'] = df['daily_revenue'].rolling(window=7).mean()
df['ma_30'] = df['daily_revenue'].rolling(window=30).mean()

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(df['date'], df['daily_revenue'], alpha=0.3, color='#90CAF9', label='Daily Revenue')
ax.plot(df['date'], df['ma_7'], color='#2196F3', linewidth=2, label='7-day MA')
ax.plot(df['date'], df['ma_30'], color='#F44336', linewidth=2, label='30-day MA')
ax.fill_between(df['date'], df['daily_revenue'], alpha=0.1, color='#2196F3')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax.set_title('Daily Revenue with Moving Averages', fontsize=16)
ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 3,
    category: 'Matplotlib',
    title: 'Scatter Plot with Regression',
    desc: 'Scatter plot with linear regression fit and confidence interval',
    code: `import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

df = spark.sql("SELECT age, income, education_level FROM customers").toPandas()

fig, ax = plt.subplots(figsize=(10, 8))
colors = {'High School': '#FF9800', 'Bachelor': '#2196F3', 'Master': '#4CAF50', 'PhD': '#9C27B0'}
for level, color in colors.items():
    mask = df['education_level'] == level
    ax.scatter(df.loc[mask, 'age'], df.loc[mask, 'income'], c=color, label=level, alpha=0.6, s=50)

slope, intercept, r, p, se = stats.linregress(df['age'], df['income'])
x_line = np.linspace(df['age'].min(), df['age'].max(), 100)
y_line = slope * x_line + intercept
ax.plot(x_line, y_line, 'r--', linewidth=2, label=f'Fit (R²={r**2:.3f})')
ax.set_title('Income vs Age by Education Level', fontsize=16)
ax.set_xlabel('Age', fontsize=12)
ax.set_ylabel('Income ($)', fontsize=12)
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 4,
    category: 'Matplotlib',
    title: 'Histogram with KDE',
    desc: 'Distribution histogram with kernel density estimation overlay',
    code: `import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gaussian_kde

df = spark.sql("SELECT transaction_amount FROM transactions WHERE transaction_amount > 0").toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
n, bins, patches = ax.hist(df['transaction_amount'], bins=50, density=True, alpha=0.7,
                            color='#2196F3', edgecolor='white', linewidth=0.5)
kde = gaussian_kde(df['transaction_amount'])
x_range = np.linspace(df['transaction_amount'].min(), df['transaction_amount'].max(), 200)
ax.plot(x_range, kde(x_range), color='#F44336', linewidth=2, label='KDE')

mean_val = df['transaction_amount'].mean()
median_val = df['transaction_amount'].median()
ax.axvline(mean_val, color='#FF9800', linestyle='--', linewidth=2, label=f'Mean: \${mean_val:.2f}')
ax.axvline(median_val, color='#4CAF50', linestyle='--', linewidth=2, label=f'Median: \${median_val:.2f}')
ax.set_title('Transaction Amount Distribution', fontsize=16)
ax.set_xlabel('Amount ($)', fontsize=12)
ax.set_ylabel('Density', fontsize=12)
ax.legend()
plt.tight_layout()
display(fig)`,
  },
  {
    id: 5,
    category: 'Matplotlib',
    title: 'Pie / Donut Chart',
    desc: 'Donut chart showing market share distribution',
    code: `import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT product_category, SUM(revenue) as total_revenue
    FROM sales GROUP BY product_category ORDER BY total_revenue DESC LIMIT 8
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 8))
colors = plt.cm.Set3(range(len(df)))
wedges, texts, autotexts = ax.pie(
    df['total_revenue'], labels=df['product_category'],
    autopct='%1.1f%%', colors=colors, startangle=90,
    pctdistance=0.85, wedgeprops=dict(width=0.4, edgecolor='white')
)
centre_circle = plt.Circle((0, 0), 0.45, fc='white')
ax.add_artist(centre_circle)
total = df['total_revenue'].sum()
ax.text(0, 0, f'Total\\n\${total/1e6:.1f}M', ha='center', va='center', fontsize=14, fontweight='bold')
ax.set_title('Revenue by Product Category', fontsize=16, fontweight='bold')
plt.setp(autotexts, size=9, weight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 6,
    category: 'Matplotlib',
    title: 'Heatmap - Correlation Matrix',
    desc: 'Custom heatmap of feature correlations with annotations',
    code: `import matplotlib.pyplot as plt
import numpy as np

df = spark.sql("SELECT * FROM feature_table").toPandas()
numeric_cols = df.select_dtypes(include=[np.number]).columns
corr_matrix = df[numeric_cols].corr()

fig, ax = plt.subplots(figsize=(14, 12))
im = ax.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1)
cbar = fig.colorbar(im, ax=ax, shrink=0.8)
cbar.set_label('Correlation Coefficient', fontsize=12)

ax.set_xticks(range(len(numeric_cols)))
ax.set_yticks(range(len(numeric_cols)))
ax.set_xticklabels(numeric_cols, rotation=45, ha='right', fontsize=9)
ax.set_yticklabels(numeric_cols, fontsize=9)

for i in range(len(numeric_cols)):
    for j in range(len(numeric_cols)):
        val = corr_matrix.iloc[i, j]
        color = 'white' if abs(val) > 0.5 else 'black'
        ax.text(j, i, f'{val:.2f}', ha='center', va='center', color=color, fontsize=8)

ax.set_title('Feature Correlation Heatmap', fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 7,
    category: 'Matplotlib',
    title: 'Subplots Grid Layout',
    desc: 'Multi-panel figure with different chart types in a grid',
    code: `import matplotlib.pyplot as plt
import numpy as np

df = spark.sql("SELECT * FROM sales_summary").toPandas()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Panel 1: Bar chart
axes[0, 0].bar(df['region'], df['revenue'], color='#2196F3')
axes[0, 0].set_title('Revenue by Region', fontweight='bold')
axes[0, 0].tick_params(axis='x', rotation=45)

# Panel 2: Line chart
axes[0, 1].plot(df['month'], df['monthly_growth'], marker='o', color='#4CAF50')
axes[0, 1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
axes[0, 1].set_title('Monthly Growth Rate', fontweight='bold')
axes[0, 1].fill_between(df['month'], df['monthly_growth'], alpha=0.2, color='#4CAF50')

# Panel 3: Histogram
axes[1, 0].hist(df['order_value'], bins=30, color='#FF9800', edgecolor='white')
axes[1, 0].set_title('Order Value Distribution', fontweight='bold')
axes[1, 0].set_xlabel('Order Value ($)')

# Panel 4: Scatter
scatter = axes[1, 1].scatter(df['marketing_spend'], df['revenue'], c=df['profit_margin'],
                              cmap='RdYlGn', s=80, alpha=0.7)
fig.colorbar(scatter, ax=axes[1, 1], label='Profit Margin')
axes[1, 1].set_title('Marketing Spend vs Revenue', fontweight='bold')

fig.suptitle('Sales Dashboard Overview', fontsize=18, fontweight='bold', y=1.02)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 8,
    category: 'Matplotlib',
    title: '3D Surface Plot',
    desc: 'Three-dimensional surface visualization for multivariate data',
    code: `import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

df = spark.sql("""
    SELECT temperature, humidity, AVG(energy_consumption) as avg_energy
    FROM sensor_readings GROUP BY temperature, humidity
""").toPandas()

fig = plt.figure(figsize=(14, 10))
ax = fig.add_subplot(111, projection='3d')

temp_grid = np.linspace(df['temperature'].min(), df['temperature'].max(), 50)
hum_grid = np.linspace(df['humidity'].min(), df['humidity'].max(), 50)
T, H = np.meshgrid(temp_grid, hum_grid)

from scipy.interpolate import griddata
Z = griddata((df['temperature'], df['humidity']), df['avg_energy'], (T, H), method='cubic')

surf = ax.plot_surface(T, H, Z, cmap='viridis', alpha=0.8, edgecolor='none')
ax.scatter(df['temperature'], df['humidity'], df['avg_energy'], c='red', s=20, alpha=0.5)
fig.colorbar(surf, ax=ax, shrink=0.5, label='Energy Consumption (kWh)')
ax.set_xlabel('Temperature (C)', fontsize=11)
ax.set_ylabel('Humidity (%)', fontsize=11)
ax.set_zlabel('Energy (kWh)', fontsize=11)
ax.set_title('Energy Consumption by Temperature & Humidity', fontsize=14)
ax.view_init(elev=30, azim=225)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 9,
    category: 'Seaborn',
    title: 'Pair Plot',
    desc: 'Pairwise relationships between numeric features colored by target',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT sepal_length, sepal_width, petal_length, petal_width, species
    FROM iris_dataset
""").toPandas()

g = sns.pairplot(df, hue='species', palette='Set2', diag_kind='kde',
                  plot_kws={'alpha': 0.6, 's': 40, 'edgecolor': 'white'},
                  diag_kws={'linewidth': 2})
g.fig.suptitle('Iris Dataset Feature Relationships', y=1.02, fontsize=16, fontweight='bold')
g.fig.set_size_inches(14, 12)
for ax in g.axes.flatten():
    ax.grid(True, alpha=0.3)
plt.tight_layout()
display(g.fig)`,
  },
  {
    id: 10,
    category: 'Seaborn',
    title: 'Box Plot - Multi Group',
    desc: 'Grouped box plots with statistical annotations',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT department, quarter, salary
    FROM employee_quarterly_data
""").toPandas()

fig, ax = plt.subplots(figsize=(14, 7))
sns.boxplot(data=df, x='department', y='salary', hue='quarter',
            palette='coolwarm', width=0.7, fliersize=3, linewidth=1.2, ax=ax)
ax.set_title('Salary Distribution by Department & Quarter', fontsize=16, fontweight='bold')
ax.set_xlabel('Department', fontsize=12)
ax.set_ylabel('Salary ($)', fontsize=12)
ax.legend(title='Quarter', loc='upper right')
ax.grid(axis='y', alpha=0.3)
ax.tick_params(axis='x', rotation=30)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 11,
    category: 'Seaborn',
    title: 'Violin Plot',
    desc: 'Violin plots showing distribution shape with inner box plots',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT product_category, customer_rating, region
    FROM reviews WHERE customer_rating IS NOT NULL
""").toPandas()

fig, ax = plt.subplots(figsize=(14, 7))
sns.violinplot(data=df, x='product_category', y='customer_rating', hue='region',
               split=True, inner='box', palette='muted', linewidth=1, ax=ax)
ax.set_title('Customer Rating Distribution by Category & Region', fontsize=16, fontweight='bold')
ax.set_xlabel('Product Category', fontsize=12)
ax.set_ylabel('Customer Rating', fontsize=12)
ax.legend(title='Region')
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 12,
    category: 'Seaborn',
    title: 'Swarm Plot',
    desc: 'Swarm plot overlaid on box plot for granular distribution view',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT job_title, years_experience, performance_score
    FROM hr_data WHERE years_experience < 20
    ORDER BY RAND() LIMIT 500
""").toPandas()

fig, ax = plt.subplots(figsize=(14, 7))
sns.boxplot(data=df, x='job_title', y='years_experience', color='lightgray',
            width=0.5, fliersize=0, ax=ax)
sns.swarmplot(data=df, x='job_title', y='years_experience', hue='performance_score',
              palette='RdYlGn', size=4, alpha=0.7, ax=ax)
ax.set_title('Experience Distribution by Job Title (colored by Performance)', fontsize=14, fontweight='bold')
ax.set_xlabel('Job Title', fontsize=12)
ax.set_ylabel('Years of Experience', fontsize=12)
ax.legend(title='Performance', bbox_to_anchor=(1.05, 1), loc='upper left')
ax.tick_params(axis='x', rotation=30)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 13,
    category: 'Seaborn',
    title: 'Joint Plot',
    desc: 'Joint distribution plot with marginal histograms and KDE',
    code: `import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import pearsonr

df = spark.sql("""
    SELECT log_duration, page_views, bounce_rate
    FROM web_analytics ORDER BY RAND() LIMIT 2000
""").toPandas()

g = sns.jointplot(data=df, x='log_duration', y='page_views',
                   kind='scatter', color='#2196F3', alpha=0.5, height=10,
                   marginal_kws={'fill': True, 'alpha': 0.5})
g.ax_joint.set_xlabel('Session Duration (log)', fontsize=12)
g.ax_joint.set_ylabel('Page Views', fontsize=12)
g.fig.suptitle('Session Duration vs Page Views', fontsize=16, fontweight='bold', y=1.02)

r, p = pearsonr(df['log_duration'], df['page_views'])
g.ax_joint.annotate(f'r = {r:.3f}, p = {p:.2e}', xy=(0.05, 0.95), xycoords='axes fraction',
                     fontsize=12, bbox=dict(boxstyle='round', fc='wheat', alpha=0.5))
display(g.fig)`,
  },
  {
    id: 14,
    category: 'Seaborn',
    title: 'FacetGrid - Multi Panel',
    desc: 'FacetGrid for comparing distributions across multiple dimensions',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT region, product_line, month, revenue
    FROM monthly_sales WHERE year = 2024
""").toPandas()

g = sns.FacetGrid(df, col='region', row='product_line', hue='product_line',
                   palette='Set2', height=3, aspect=1.5, margin_titles=True)
g.map_dataframe(sns.lineplot, x='month', y='revenue', marker='o', linewidth=2)
g.set_axis_labels('Month', 'Revenue ($)')
g.set_titles(col_template='{col_name}', row_template='{row_name}')
g.add_legend(title='Product Line')
g.fig.suptitle('Monthly Revenue: Region x Product Line', fontsize=16, fontweight='bold', y=1.02)
for ax in g.axes.flat:
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3)
plt.tight_layout()
display(g.fig)`,
  },
  {
    id: 15,
    category: 'Seaborn',
    title: 'Clustermap - Hierarchical',
    desc: 'Hierarchical clustering heatmap with dendrograms',
    code: `import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import StandardScaler

df = spark.sql("SELECT * FROM gene_expression_matrix").toPandas()
df = df.set_index('gene_name')
df_numeric = df.select_dtypes(include='number')

scaler = StandardScaler()
df_scaled = pd.DataFrame(scaler.fit_transform(df_numeric),
                          index=df_numeric.index, columns=df_numeric.columns)

g = sns.clustermap(df_scaled, method='ward', metric='euclidean',
                    cmap='RdBu_r', figsize=(14, 12), linewidths=0.5,
                    row_cluster=True, col_cluster=True,
                    dendrogram_ratio=(0.15, 0.15),
                    cbar_kws={'label': 'Z-score'})
g.fig.suptitle('Gene Expression Clustermap', fontsize=16, fontweight='bold', y=1.02)
plt.setp(g.ax_heatmap.get_xticklabels(), rotation=45, ha='right', fontsize=8)
plt.setp(g.ax_heatmap.get_yticklabels(), fontsize=8)
display(g.fig)`,
  },
  {
    id: 16,
    category: 'Seaborn',
    title: 'Regression Plot',
    desc: 'Linear and polynomial regression visualization with confidence bands',
    code: `import seaborn as sns
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT square_footage, price, bedrooms, neighborhood
    FROM real_estate WHERE price < 2000000
""").toPandas()

fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# Linear regression
sns.regplot(data=df, x='square_footage', y='price', ax=axes[0],
            scatter_kws={'alpha': 0.3, 's': 20}, line_kws={'color': 'red'}, ci=95)
axes[0].set_title('Linear Regression', fontweight='bold')

# Polynomial regression (order=2)
sns.regplot(data=df, x='square_footage', y='price', ax=axes[1], order=2,
            scatter_kws={'alpha': 0.3, 's': 20}, line_kws={'color': 'green'}, ci=95)
axes[1].set_title('Polynomial (degree=2)', fontweight='bold')

# Lowess smoothing
sns.regplot(data=df, x='square_footage', y='price', ax=axes[2], lowess=True,
            scatter_kws={'alpha': 0.3, 's': 20}, line_kws={'color': 'purple'})
axes[2].set_title('LOWESS Smoothing', fontweight='bold')

fig.suptitle('Real Estate Price Regression Analysis', fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 17,
    category: 'Plotly',
    title: 'Interactive Scatter Plot',
    desc: 'Interactive scatter with hover data, color scale, and size mapping',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT company_name, revenue, profit_margin, num_employees, industry, market_cap
    FROM company_financials
""").toPandas()

fig = px.scatter(df, x='revenue', y='profit_margin', size='num_employees',
                  color='industry', hover_name='company_name',
                  hover_data=['market_cap'],
                  size_max=60, opacity=0.7,
                  color_discrete_sequence=px.colors.qualitative.Set2,
                  title='Company Performance: Revenue vs Profit Margin')
fig.update_layout(
    xaxis_title='Revenue ($M)', yaxis_title='Profit Margin (%)',
    width=1000, height=700,
    legend=dict(orientation='h', yanchor='bottom', y=-0.3)
)
fig.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')))
fig.show()`,
  },
  {
    id: 18,
    category: 'Plotly',
    title: 'Animated Bar Chart',
    desc: 'Animated bar chart showing ranking changes over time',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT year, country, gdp, continent
    FROM world_gdp WHERE year BETWEEN 2000 AND 2023
    ORDER BY year, gdp DESC
""").toPandas()

# Keep top 10 per year
df['rank'] = df.groupby('year')['gdp'].rank(ascending=False, method='first')
df = df[df['rank'] <= 10]

fig = px.bar(df, x='gdp', y='country', color='continent',
              animation_frame='year', animation_group='country',
              orientation='h', range_x=[0, df['gdp'].max() * 1.1],
              title='Top 10 Countries by GDP Over Time',
              color_discrete_sequence=px.colors.qualitative.Bold)
fig.update_layout(
    yaxis=dict(categoryorder='total ascending', autorange=True),
    width=1000, height=600,
    xaxis_title='GDP ($B)', yaxis_title='',
    transition={'duration': 500}
)
fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 800
fig.show()`,
  },
  {
    id: 19,
    category: 'Plotly',
    title: 'Sunburst Chart',
    desc: 'Hierarchical sunburst showing organizational revenue breakdown',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT region, country, city, SUM(revenue) as revenue
    FROM sales_hierarchy
    GROUP BY region, country, city
""").toPandas()

fig = px.sunburst(df, path=['region', 'country', 'city'], values='revenue',
                   color='revenue', color_continuous_scale='Viridis',
                   title='Revenue Hierarchy: Region > Country > City')
fig.update_layout(width=900, height=900)
fig.update_traces(textinfo='label+percent parent',
                   insidetextorientation='radial',
                   hovertemplate='<b>%{label}</b><br>Revenue: $%{value:,.0f}<br>Parent: %{parent}')
fig.show()`,
  },
  {
    id: 20,
    category: 'Plotly',
    title: 'Treemap',
    desc: 'Treemap visualization of portfolio allocation or file system usage',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT sector, industry, company, market_cap, ytd_return
    FROM stock_portfolio
""").toPandas()

fig = px.treemap(df, path=['sector', 'industry', 'company'],
                  values='market_cap', color='ytd_return',
                  color_continuous_scale='RdYlGn',
                  color_continuous_midpoint=0,
                  title='Portfolio Allocation (colored by YTD Return %)')
fig.update_layout(width=1100, height=800)
fig.update_traces(
    texttemplate='<b>%{label}</b><br>$%{value:,.0f}<br>%{color:.1f}%',
    hovertemplate='<b>%{label}</b><br>Market Cap: $%{value:,.0f}<br>YTD Return: %{color:.1f}%'
)
fig.show()`,
  },
  {
    id: 21,
    category: 'Plotly',
    title: 'Sankey Diagram',
    desc: 'Flow diagram showing data pipeline or customer journey transitions',
    code: `import plotly.graph_objects as go

df = spark.sql("""
    SELECT source_stage, target_stage, user_count
    FROM customer_journey_flows
""").toPandas()

all_nodes = list(set(df['source_stage'].tolist() + df['target_stage'].tolist()))
node_map = {name: i for i, name in enumerate(all_nodes)}

fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=20, thickness=30, line=dict(color='black', width=0.5),
        label=all_nodes,
        color=['#2196F3', '#4CAF50', '#FF9800', '#F44336', '#9C27B0',
               '#00BCD4', '#795548', '#607D8B'][:len(all_nodes)]
    ),
    link=dict(
        source=[node_map[s] for s in df['source_stage']],
        target=[node_map[t] for t in df['target_stage']],
        value=df['user_count'].tolist(),
        color='rgba(100,100,100,0.2)'
    )
)])
fig.update_layout(title='Customer Journey Flow', font_size=12, width=1000, height=600)
fig.show()`,
  },
  {
    id: 22,
    category: 'Plotly',
    title: 'Funnel Chart',
    desc: 'Sales or conversion funnel with drop-off analysis',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT stage, user_count, conversion_rate
    FROM conversion_funnel ORDER BY stage_order
""").toPandas()

fig = px.funnel(df, x='user_count', y='stage',
                 color='conversion_rate',
                 color_continuous_scale='Blues',
                 title='Sales Conversion Funnel')
fig.update_layout(width=900, height=500)
fig.update_traces(
    texttemplate='%{x:,.0f} (%{percentInitial:.1%})',
    textposition='inside',
    marker=dict(line=dict(width=2, color='white'))
)

# Add drop-off annotations
for i in range(1, len(df)):
    drop = df.iloc[i-1]['user_count'] - df.iloc[i]['user_count']
    pct = drop / df.iloc[i-1]['user_count'] * 100
    fig.add_annotation(x=df.iloc[i]['user_count'], y=df.iloc[i]['stage'],
                        text=f'drop {pct:.1f}%', showarrow=False, xanchor='left', xshift=10)
fig.show()`,
  },
  {
    id: 23,
    category: 'Plotly',
    title: 'Waterfall Chart',
    desc: 'Waterfall chart showing cumulative financial impact',
    code: `import plotly.graph_objects as go

df = spark.sql("""
    SELECT item, amount, category
    FROM financial_waterfall ORDER BY sort_order
""").toPandas()

measure = ['relative' if cat != 'total' else 'total' for cat in df['category']]

fig = go.Figure(go.Waterfall(
    name='Financials',
    orientation='v',
    measure=measure,
    x=df['item'],
    y=df['amount'],
    connector=dict(line=dict(color='rgb(63, 63, 63)', width=1)),
    increasing=dict(marker=dict(color='#4CAF50')),
    decreasing=dict(marker=dict(color='#F44336')),
    totals=dict(marker=dict(color='#2196F3')),
    textposition='outside',
    text=[f'\${v/1e6:+.1f}M' if abs(v) > 1e6 else f'\${v:+,.0f}' for v in df['amount']]
))
fig.update_layout(
    title='Financial Waterfall Analysis', width=1000, height=600,
    yaxis_title='Amount ($)', showlegend=False,
    xaxis_tickangle=-30
)
fig.show()`,
  },
  {
    id: 24,
    category: 'Plotly',
    title: 'Candlestick Chart',
    desc: 'OHLC candlestick chart for stock price analysis',
    code: `import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

df = spark.sql("""
    SELECT date, open, high, low, close, volume
    FROM stock_prices WHERE ticker = 'AAPL'
    ORDER BY date
""").toPandas()
df['date'] = pd.to_datetime(df['date'])
df['ma_20'] = df['close'].rolling(20).mean()
df['ma_50'] = df['close'].rolling(50).mean()

fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                     vertical_spacing=0.05, subplot_titles=('AAPL Stock Price', 'Volume'))

fig.add_trace(go.Candlestick(x=df['date'], open=df['open'], high=df['high'],
              low=df['low'], close=df['close'], name='OHLC'), row=1, col=1)
fig.add_trace(go.Scatter(x=df['date'], y=df['ma_20'], name='MA-20',
              line=dict(color='orange', width=1.5)), row=1, col=1)
fig.add_trace(go.Scatter(x=df['date'], y=df['ma_50'], name='MA-50',
              line=dict(color='blue', width=1.5)), row=1, col=1)

colors = ['red' if c < o else 'green' for c, o in zip(df['close'], df['open'])]
fig.add_trace(go.Bar(x=df['date'], y=df['volume'], name='Volume',
              marker_color=colors, opacity=0.5), row=2, col=1)

fig.update_layout(width=1100, height=700, xaxis_rangeslider_visible=False)
fig.show()`,
  },
  {
    id: 25,
    category: 'Plotly',
    title: 'Radar / Spider Chart',
    desc: 'Radar chart comparing multi-dimensional performance metrics',
    code: `import plotly.graph_objects as go

df = spark.sql("""
    SELECT team, reliability, speed, quality, innovation, collaboration, efficiency
    FROM team_performance_scores
""").toPandas()

categories = ['Reliability', 'Speed', 'Quality', 'Innovation', 'Collaboration', 'Efficiency']
fig = go.Figure()

colors = ['#2196F3', '#FF5722', '#4CAF50', '#9C27B0']
for i, row in df.iterrows():
    values = [row[c.lower()] for c in categories]
    values.append(values[0])  # close the polygon
    fig.add_trace(go.Scatterpolar(
        r=values, theta=categories + [categories[0]],
        fill='toself', name=row['team'],
        line=dict(color=colors[i % len(colors)], width=2),
        fillcolor=colors[i % len(colors)], opacity=0.3
    ))

fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 100], ticksuffix='%')),
    title='Team Performance Comparison', width=800, height=700,
    showlegend=True, legend=dict(orientation='h', y=-0.1)
)
fig.show()`,
  },
  {
    id: 26,
    category: 'Plotly',
    title: 'Choropleth Map',
    desc: 'Geographic choropleth map of sales data by state or country',
    code: `import plotly.express as px

df = spark.sql("""
    SELECT state_code, state_name, total_sales, avg_order_value, num_customers
    FROM state_sales_summary
""").toPandas()

fig = px.choropleth(df, locations='state_code', locationmode='USA-states',
                     color='total_sales', scope='usa',
                     color_continuous_scale='Viridis',
                     hover_name='state_name',
                     hover_data={'avg_order_value': ':$.2f', 'num_customers': ':,'},
                     title='Total Sales by State')
fig.update_layout(
    geo=dict(bgcolor='rgba(0,0,0,0)', lakecolor='lightblue',
             showlakes=True, showland=True, landcolor='rgb(240,240,240)'),
    width=1100, height=700,
    coloraxis_colorbar=dict(title='Sales ($)')
)
fig.update_traces(marker_line_color='white', marker_line_width=0.5)
fig.show()`,
  },
  {
    id: 27,
    category: 'Databricks Native',
    title: 'display() - DataFrame Visualization',
    desc: 'Use Databricks display() for interactive DataFrame charts',
    code: `# Databricks native display() auto-detects chart types
df = spark.sql("""
    SELECT date, product_category, SUM(revenue) as revenue, COUNT(*) as orders
    FROM sales
    GROUP BY date, product_category
    ORDER BY date
""")

# Basic table display
display(df)

# Display with built-in chart - click "+" to add visualization
# Then select Chart Type: Bar, Line, Scatter, Pie, Map, etc.
# Configure: Keys = date, Values = revenue, Group By = product_category

# Profile display - shows data distribution stats
display(df.summary())

# Limit display for large datasets
display(df.limit(1000))

# Display with specific columns
display(df.select("date", "revenue", "product_category"))`,
  },
  {
    id: 28,
    category: 'Databricks Native',
    title: 'displayHTML() - Custom HTML/JS',
    desc: 'Render custom HTML, CSS, and JavaScript visualizations in notebooks',
    code: `# Render custom HTML dashboard with displayHTML()
html_content = \"\"\"
<style>
  .kpi-container { display: flex; gap: 20px; padding: 20px; }
  .kpi-card {
    flex: 1; padding: 20px; border-radius: 12px;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white; text-align: center;
    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
  }
  .kpi-value { font-size: 36px; font-weight: bold; margin: 10px 0; }
  .kpi-label { font-size: 14px; opacity: 0.9; }
  .kpi-change { font-size: 12px; margin-top: 5px; }
  .positive { color: #00ff88; }
  .negative { color: #ff4444; }
</style>
<div class="kpi-container">
  <div class="kpi-card">
    <div class="kpi-label">Total Revenue</div>
    <div class="kpi-value">$2.4M</div>
    <div class="kpi-change positive">+12.5% vs last month</div>
  </div>
  <div class="kpi-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
    <div class="kpi-label">Active Users</div>
    <div class="kpi-value">45,231</div>
    <div class="kpi-change positive">+8.3% vs last month</div>
  </div>
  <div class="kpi-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
    <div class="kpi-label">Conversion Rate</div>
    <div class="kpi-value">3.8%</div>
    <div class="kpi-change negative">-0.2% vs last month</div>
  </div>
  <div class="kpi-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
    <div class="kpi-label">Avg Order Value</div>
    <div class="kpi-value">$127</div>
    <div class="kpi-change positive">+5.1% vs last month</div>
  </div>
</div>
\"\"\"
displayHTML(html_content)`,
  },
  {
    id: 29,
    category: 'Databricks Native',
    title: 'Notebook Charts - Built-in Types',
    desc: 'Configure Databricks built-in chart types from notebook results',
    code: `# Databricks notebooks support built-in chart configurations
# After running display(), click the chart icon to configure

# Example 1: Stacked Area Chart
df_time = spark.sql("""
    SELECT date_trunc('week', order_date) as week,
           product_line,
           SUM(quantity) as total_qty
    FROM orders GROUP BY 1, 2 ORDER BY 1
""")
display(df_time)
# Chart Config: Type=Area, Keys=week, Values=total_qty, Group=product_line, Stacked=True

# Example 2: Pivot Table visualization
df_pivot = spark.sql("""
    SELECT region, quarter,
           SUM(revenue) as revenue,
           AVG(margin) as avg_margin
    FROM regional_sales GROUP BY region, quarter
""")
display(df_pivot)
# Chart Config: Type=Bar, Keys=region, Values=revenue, Group=quarter

# Example 3: Map visualization
df_geo = spark.sql("""
    SELECT latitude, longitude, city, population
    FROM city_data WHERE latitude IS NOT NULL
""")
display(df_geo)
# Chart Config: Type=Map, Lat=latitude, Lon=longitude, Size=population`,
  },
  {
    id: 30,
    category: 'Databricks Native',
    title: 'Widgets for Interactive Filters',
    desc: 'Create interactive widgets for parameterized visualizations',
    code: `# Create Databricks widgets for interactive exploration
dbutils.widgets.dropdown("metric", "revenue", ["revenue", "profit", "orders", "customers"])
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("end_date", "2024-12-31")
dbutils.widgets.multiselect("regions", "ALL", ["ALL", "North", "South", "East", "West"])
dbutils.widgets.dropdown("chart_type", "bar", ["bar", "line", "area", "scatter"])

metric = dbutils.widgets.get("metric")
start = dbutils.widgets.get("start_date")
end = dbutils.widgets.get("end_date")
regions = dbutils.widgets.get("regions")

region_filter = ""
if "ALL" not in regions:
    region_list = ",".join([f"'{r}'" for r in regions.split(",")])
    region_filter = f"AND region IN ({region_list})"

df = spark.sql(f"""
    SELECT date_trunc('month', sale_date) as month, region,
           SUM({metric}) as value
    FROM sales
    WHERE sale_date BETWEEN '{start}' AND '{end}' {region_filter}
    GROUP BY 1, 2 ORDER BY 1
""")
display(df)

# Widget values persist across notebook runs
# Remove widgets when done: dbutils.widgets.removeAll()`,
  },
  {
    id: 31,
    category: 'Databricks Native',
    title: 'Databricks SQL Dashboard',
    desc: 'Build SQL-based dashboards using Databricks SQL endpoints',
    code: `# Databricks SQL Dashboard Configuration
# Step 1: Create queries in Databricks SQL Editor

# Query 1: KPI Summary
# -- SQL
# SELECT
#     COUNT(DISTINCT customer_id) as total_customers,
#     SUM(revenue) as total_revenue,
#     AVG(order_value) as avg_order_value,
#     SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL 30 DAYS
#         THEN revenue ELSE 0 END) as last_30_days_revenue
# FROM gold.sales_facts;

# Query 2: Revenue Trend
# -- SQL
# SELECT date_trunc('week', order_date) as week,
#        SUM(revenue) as weekly_revenue,
#        COUNT(*) as order_count
# FROM gold.sales_facts
# WHERE order_date >= CURRENT_DATE - INTERVAL 6 MONTHS
# GROUP BY 1 ORDER BY 1;

# Query 3: Top Products
# -- SQL
# SELECT p.product_name, p.category,
#        SUM(s.quantity) as units_sold,
#        SUM(s.revenue) as total_revenue
# FROM gold.sales_facts s JOIN gold.products p ON s.product_id = p.id
# GROUP BY 1, 2 ORDER BY total_revenue DESC LIMIT 20;

# Step 2: Create Dashboard from SQL Editor -> New Dashboard
# Step 3: Add each query as a widget (counter, chart, table)
# Step 4: Schedule auto-refresh every 15 minutes
print("Dashboard created - navigate to Databricks SQL > Dashboards to view")`,
  },
  {
    id: 32,
    category: 'Databricks Native',
    title: 'Notebook Results as HTML Table',
    desc: 'Format DataFrame results as styled HTML tables with conditional formatting',
    code: `import pandas as pd

df = spark.sql("""
    SELECT product, region, q1_sales, q2_sales, q3_sales, q4_sales,
           (q1_sales + q2_sales + q3_sales + q4_sales) as total
    FROM quarterly_sales ORDER BY total DESC LIMIT 15
""").toPandas()

def color_performance(val):
    if isinstance(val, (int, float)):
        if val > 100000:
            return 'background-color: #c8e6c9; color: #2e7d32'
        elif val > 50000:
            return 'background-color: #fff9c4; color: #f57f17'
        else:
            return 'background-color: #ffcdd2; color: #c62828'
    return ''

styled = df.style \\
    .applymap(color_performance,
              subset=['q1_sales', 'q2_sales', 'q3_sales', 'q4_sales', 'total']) \\
    .format({'q1_sales': '\${:,.0f}', 'q2_sales': '\${:,.0f}',
             'q3_sales': '\${:,.0f}', 'q4_sales': '\${:,.0f}', 'total': '\${:,.0f}'}) \\
    .set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#1a237e'), ('color', 'white'),
                                      ('font-weight', 'bold'), ('text-align', 'center')]},
        {'selector': 'td', 'props': [('text-align', 'right'), ('padding', '8px')]}
    ]) \\
    .bar(subset=['total'], color='#bbdefb') \\
    .set_caption('Quarterly Sales Performance Report')

displayHTML(styled.to_html())`,
  },
  {
    id: 33,
    category: 'Pandas Plots',
    title: 'Correlation Matrix Heatmap',
    desc: 'Pandas-based correlation analysis with styled output',
    code: `import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

df = spark.sql("SELECT * FROM ml_features").toPandas()
numeric_df = df.select_dtypes(include=[np.number])
corr = numeric_df.corr()

# Mask upper triangle
mask = np.triu(np.ones_like(corr, dtype=bool))

fig, ax = plt.subplots(figsize=(14, 11))
sns.heatmap(corr, mask=mask, annot=True, fmt='.2f', cmap='coolwarm',
            center=0, square=True, linewidths=0.5,
            cbar_kws={'shrink': 0.8, 'label': 'Correlation'},
            annot_kws={'size': 8}, vmin=-1, vmax=1, ax=ax)

ax.set_title('Feature Correlation Matrix (Lower Triangle)', fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right', fontsize=9)
plt.yticks(fontsize=9)

# Highlight strong correlations
strong = corr[(abs(corr) > 0.7) & (corr != 1.0)].stack().reset_index()
if len(strong) > 0:
    print(f"Strong correlations (|r| > 0.7): {len(strong)} pairs found")
plt.tight_layout()
display(fig)`,
  },
  {
    id: 34,
    category: 'Pandas Plots',
    title: 'Distribution Analysis',
    desc: 'Multi-feature distribution plots with skewness and kurtosis stats',
    code: `import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import numpy as np

df = spark.sql("SELECT * FROM customer_metrics").toPandas()
features = ['age', 'income', 'spending_score', 'loyalty_years', 'transaction_count']

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
axes = axes.flatten()

for i, feat in enumerate(features):
    ax = axes[i]
    data = df[feat].dropna()

    ax.hist(data, bins=40, density=True, alpha=0.7, color='#2196F3', edgecolor='white')

    # Fit normal distribution
    mu, sigma = data.mean(), data.std()
    x = np.linspace(data.min(), data.max(), 100)
    ax.plot(x, stats.norm.pdf(x, mu, sigma), 'r-', linewidth=2, label='Normal Fit')

    skew = data.skew()
    kurt = data.kurtosis()
    ax.set_title(f'{feat}\\nSkew={skew:.2f}, Kurt={kurt:.2f}', fontsize=11, fontweight='bold')
    ax.axvline(mu, color='orange', linestyle='--', label=f'Mean={mu:.1f}')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

axes[-1].axis('off')
fig.suptitle('Feature Distribution Analysis', fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 35,
    category: 'Pandas Plots',
    title: 'Time Series Decomposition',
    desc: 'Decompose time series into trend, seasonal, and residual components',
    code: `import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose

df = spark.sql("""
    SELECT date, daily_sales FROM daily_metrics ORDER BY date
""").toPandas()
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date')
df = df.asfreq('D').fillna(method='ffill')

decomposition = seasonal_decompose(df['daily_sales'], model='multiplicative', period=30)

fig, axes = plt.subplots(4, 1, figsize=(16, 14), sharex=True)
components = [
    ('Observed', decomposition.observed, '#2196F3'),
    ('Trend', decomposition.trend, '#4CAF50'),
    ('Seasonal', decomposition.seasonal, '#FF9800'),
    ('Residual', decomposition.resid, '#F44336')
]
for ax, (title, data, color) in zip(axes, components):
    ax.plot(data, color=color, linewidth=1.5)
    ax.set_ylabel(title, fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)
    if title == 'Residual':
        ax.axhline(y=1, color='gray', linestyle='--')

fig.suptitle('Time Series Decomposition (Multiplicative)', fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 36,
    category: 'Pandas Plots',
    title: 'Stacked Bar Chart',
    desc: 'Stacked and grouped bar charts for composition analysis',
    code: `import pandas as pd
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT region, product_a, product_b, product_c, product_d
    FROM regional_product_sales ORDER BY region
""").toPandas()
df = df.set_index('region')

fig, axes = plt.subplots(1, 2, figsize=(18, 7))

# Stacked bar
df.plot(kind='bar', stacked=True, ax=axes[0],
        color=['#2196F3', '#4CAF50', '#FF9800', '#F44336'],
        edgecolor='white', linewidth=0.5)
axes[0].set_title('Stacked: Revenue by Region & Product', fontweight='bold', fontsize=13)
axes[0].set_ylabel('Revenue ($)')
axes[0].tick_params(axis='x', rotation=45)
axes[0].legend(title='Product', bbox_to_anchor=(1, 1))

# Percentage stacked
df_pct = df.div(df.sum(axis=1), axis=0) * 100
df_pct.plot(kind='bar', stacked=True, ax=axes[1],
            color=['#2196F3', '#4CAF50', '#FF9800', '#F44336'],
            edgecolor='white', linewidth=0.5)
axes[1].set_title('100% Stacked: Revenue Composition', fontweight='bold', fontsize=13)
axes[1].set_ylabel('Percentage (%)')
axes[1].tick_params(axis='x', rotation=45)
axes[1].legend(title='Product', bbox_to_anchor=(1, 1))
axes[1].set_ylim(0, 100)

plt.tight_layout()
display(fig)`,
  },
  {
    id: 37,
    category: 'Pandas Plots',
    title: 'Area Chart',
    desc: 'Stacked area chart for cumulative trend visualization',
    code: `import pandas as pd
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT month, web_traffic, mobile_traffic, api_traffic, partner_traffic
    FROM monthly_traffic ORDER BY month
""").toPandas()
df['month'] = pd.to_datetime(df['month'])
df = df.set_index('month')

fig, axes = plt.subplots(2, 1, figsize=(16, 10))

# Stacked area
df.plot.area(ax=axes[0], alpha=0.7,
             color=['#2196F3', '#4CAF50', '#FF9800', '#9C27B0'],
             linewidth=1.5)
axes[0].set_title('Traffic Sources - Stacked Area', fontsize=14, fontweight='bold')
axes[0].set_ylabel('Traffic Volume')
axes[0].legend(loc='upper left')
axes[0].grid(True, alpha=0.3)

# Percentage area
df_pct = df.div(df.sum(axis=1), axis=0) * 100
df_pct.plot.area(ax=axes[1], alpha=0.7,
                  color=['#2196F3', '#4CAF50', '#FF9800', '#9C27B0'],
                  linewidth=1.5)
axes[1].set_title('Traffic Sources - Percentage Share', fontsize=14, fontweight='bold')
axes[1].set_ylabel('Percentage (%)')
axes[1].set_ylim(0, 100)
axes[1].legend(loc='upper left')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
display(fig)`,
  },
  {
    id: 38,
    category: 'Pandas Plots',
    title: 'Box Plot Comparison',
    desc: 'Side-by-side box plots with statistical summary overlay',
    code: `import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = spark.sql("""
    SELECT channel, response_time_ms, status_code
    FROM api_performance_logs
    WHERE response_time_ms < 10000
""").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# Box plot by channel
bp = df.boxplot(column='response_time_ms', by='channel', ax=axes[0],
                 patch_artist=True, notch=True,
                 boxprops=dict(facecolor='#BBDEFB'),
                 medianprops=dict(color='red', linewidth=2),
                 flierprops=dict(marker='o', markerfacecolor='gray', markersize=3))
axes[0].set_title('Response Time by Channel', fontweight='bold')
axes[0].set_ylabel('Response Time (ms)')
axes[0].set_xlabel('Channel')
fig.suptitle('')

# Stats table
stats_df = df.groupby('channel')['response_time_ms'].describe().round(1)
axes[1].axis('off')
table = axes[1].table(cellText=stats_df.values,
                       rowLabels=stats_df.index,
                       colLabels=stats_df.columns,
                       cellLoc='center', loc='center')
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1.2, 1.5)
axes[1].set_title('Statistical Summary', fontweight='bold', pad=20)

plt.tight_layout()
display(fig)`,
  },
  {
    id: 39,
    category: 'Dashboard',
    title: 'KPI Cards Dashboard',
    desc: 'Build a complete KPI dashboard with sparklines and trend indicators',
    code: `import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

# Fetch KPI data
kpis = spark.sql("""
    SELECT metric_name, current_value, previous_value, target_value, unit
    FROM dashboard_kpis
""").toPandas()

fig = plt.figure(figsize=(20, 5))
gs = gridspec.GridSpec(1, 4, wspace=0.3)

colors = ['#2196F3', '#4CAF50', '#FF9800', '#9C27B0']
for i, (_, row) in enumerate(kpis.head(4).iterrows()):
    ax = fig.add_subplot(gs[0, i])
    change_pct = ((row['current_value'] - row['previous_value']) / row['previous_value']) * 100
    arrow = 'UP' if change_pct > 0 else 'DOWN'
    color_change = '#4CAF50' if change_pct > 0 else '#F44336'

    ax.text(0.5, 0.85, row['metric_name'], transform=ax.transAxes,
            fontsize=11, ha='center', color='gray', fontweight='bold')
    ax.text(0.5, 0.55, f"{row['unit']}{row['current_value']:,.0f}",
            transform=ax.transAxes, fontsize=28, ha='center', fontweight='bold', color=colors[i])
    ax.text(0.5, 0.30, f"{arrow} {abs(change_pct):.1f}% vs prev",
            transform=ax.transAxes, fontsize=11, ha='center', color=color_change)

    # Mini sparkline
    trend = np.random.randn(20).cumsum() + row['current_value']
    ax_spark = ax.inset_axes([0.1, 0.02, 0.8, 0.2])
    ax_spark.plot(trend, color=colors[i], linewidth=1.5)
    ax_spark.fill_between(range(len(trend)), trend, alpha=0.1, color=colors[i])
    ax_spark.axis('off')

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    ax.patch.set_edgecolor(colors[i])
    ax.patch.set_linewidth(2)
    ax.patch.set_facecolor('#FAFAFA')

fig.suptitle('Executive KPI Dashboard', fontsize=18, fontweight='bold', y=1.05)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 40,
    category: 'Dashboard',
    title: 'Multi-Chart Layout Dashboard',
    desc: 'Complex dashboard with multiple synchronized chart panels',
    code: `import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

df = spark.sql("SELECT * FROM sales_dashboard_data").toPandas()

fig = plt.figure(figsize=(22, 14))
gs = gridspec.GridSpec(3, 3, hspace=0.35, wspace=0.3)

# Revenue trend (top left, spans 2 cols)
ax1 = fig.add_subplot(gs[0, :2])
ax1.plot(df['month'], df['revenue'], color='#2196F3', linewidth=2, marker='o')
ax1.fill_between(range(len(df)), df['revenue'], alpha=0.1, color='#2196F3')
ax1.set_title('Monthly Revenue Trend', fontweight='bold')
ax1.grid(True, alpha=0.3)

# Pie chart (top right)
ax2 = fig.add_subplot(gs[0, 2])
segments = df.groupby('segment')['revenue'].sum()
ax2.pie(segments, labels=segments.index, autopct='%1.1f%%', colors=plt.cm.Set3.colors)
ax2.set_title('Revenue by Segment', fontweight='bold')

# Bar chart (middle left)
ax3 = fig.add_subplot(gs[1, 0])
ax3.barh(df['product'].head(8), df['units'].head(8), color='#4CAF50')
ax3.set_title('Top Products by Units', fontweight='bold')

# Heatmap (middle center + right)
ax4 = fig.add_subplot(gs[1, 1:])
heatmap_data = df.pivot_table(values='revenue', index='region', columns='quarter')
im = ax4.imshow(heatmap_data, cmap='YlOrRd', aspect='auto')
fig.colorbar(im, ax=ax4, shrink=0.8)
ax4.set_title('Revenue: Region x Quarter', fontweight='bold')

# Scatter (bottom left)
ax5 = fig.add_subplot(gs[2, 0])
ax5.scatter(df['discount'], df['profit'], c=df['quantity'], cmap='viridis', alpha=0.6)
ax5.set_title('Discount vs Profit', fontweight='bold')

# Box plot (bottom center)
ax6 = fig.add_subplot(gs[2, 1])
ax6.boxplot([df[df['region']==r]['revenue'] for r in df['region'].unique()],
            labels=df['region'].unique())
ax6.set_title('Revenue Distribution by Region', fontweight='bold')

# Table (bottom right)
ax7 = fig.add_subplot(gs[2, 2])
ax7.axis('off')
summary = df.groupby('region').agg({'revenue': 'sum', 'profit': 'sum'}).round(0)
table = ax7.table(cellText=summary.values, rowLabels=summary.index,
                   colLabels=['Revenue', 'Profit'], loc='center')
table.auto_set_font_size(False)
table.set_fontsize(10)
ax7.set_title('Regional Summary', fontweight='bold')

fig.suptitle('Sales Performance Dashboard', fontsize=20, fontweight='bold')
display(fig)`,
  },
  {
    id: 41,
    category: 'Dashboard',
    title: 'Drill-Down Interactive Dashboard',
    desc: 'Plotly dashboard with drill-down capability from summary to detail',
    code: `import plotly.graph_objects as go
from plotly.subplots import make_subplots

df = spark.sql("""
    SELECT region, state, city, product_category, revenue, units, profit
    FROM sales_detail
""").toPandas()

# Level 1: Region Summary
region_df = df.groupby('region').agg({
    'revenue': 'sum', 'profit': 'sum', 'units': 'sum'
}).reset_index()

fig = make_subplots(rows=2, cols=2,
    specs=[[{"type": "bar"}, {"type": "pie"}],
           [{"type": "scatter", "colspan": 2}, None]],
    subplot_titles=('Revenue by Region (click to drill down)', 'Profit Distribution',
                    'Revenue vs Units Scatter'))

fig.add_trace(go.Bar(x=region_df['region'], y=region_df['revenue'], name='Revenue',
              marker_color='#2196F3', customdata=region_df['region']), row=1, col=1)

fig.add_trace(go.Pie(labels=region_df['region'], values=region_df['profit'],
              hole=0.4, name='Profit'), row=1, col=2)

state_df = df.groupby(['region', 'state']).agg({
    'revenue': 'sum', 'units': 'sum'
}).reset_index()
fig.add_trace(go.Scatter(x=state_df['units'], y=state_df['revenue'], mode='markers',
              text=state_df['state'], marker=dict(size=10, color=state_df['revenue'],
              colorscale='Viridis', showscale=True), name='States'), row=2, col=1)

fig.update_layout(height=800, width=1100, title_text='Sales Drill-Down Dashboard',
                   showlegend=True, template='plotly_white')
fig.update_layout(clickmode='event+select')
fig.show()

# In Databricks: Use ipywidgets or dbutils.widgets for interactive drill-down
# dbutils.widgets.dropdown("selected_region", "All", ["All"] + region_df['region'].tolist())`,
  },
  {
    id: 42,
    category: 'Dashboard',
    title: 'Real-Time Refresh Dashboard',
    desc: 'Auto-refreshing dashboard using Databricks widgets and scheduled queries',
    code: `# Real-time dashboard with auto-refresh in Databricks
import time
import matplotlib.pyplot as plt
from IPython.display import clear_output

def build_realtime_dashboard():
    clear_output(wait=True)

    # Fetch latest metrics
    metrics = spark.sql("""
        SELECT
            COUNT(*) as total_events,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
            AVG(response_time_ms) as avg_response,
            MAX(event_time) as last_event
        FROM streaming_events
        WHERE event_time > current_timestamp() - INTERVAL 5 MINUTES
    """).toPandas().iloc[0]

    # Time series for last hour
    ts_df = spark.sql("""
        SELECT date_trunc('minute', event_time) as minute,
               COUNT(*) as event_count,
               AVG(response_time_ms) as avg_rt
        FROM streaming_events
        WHERE event_time > current_timestamp() - INTERVAL 1 HOUR
        GROUP BY 1 ORDER BY 1
    """).toPandas()

    fig, axes = plt.subplots(1, 3, figsize=(20, 5))

    # Events per minute
    axes[0].plot(ts_df['minute'], ts_df['event_count'], color='#2196F3', linewidth=2)
    axes[0].fill_between(ts_df['minute'], ts_df['event_count'], alpha=0.2, color='#2196F3')
    axes[0].set_title(f"Events/Min (Total: {metrics['total_events']:,})", fontweight='bold')
    axes[0].tick_params(axis='x', rotation=45)

    # Response time
    axes[1].plot(ts_df['minute'], ts_df['avg_rt'], color='#FF9800', linewidth=2)
    axes[1].axhline(y=200, color='red', linestyle='--', label='SLA: 200ms')
    axes[1].set_title(f"Avg Response Time ({metrics['avg_response']:.0f}ms)", fontweight='bold')
    axes[1].legend()
    axes[1].tick_params(axis='x', rotation=45)

    # Error rate gauge
    error_rate = (metrics['errors'] / max(metrics['total_events'], 1)) * 100
    axes[2].pie([error_rate, 100-error_rate], labels=['Errors', 'Success'],
                colors=['#F44336', '#4CAF50'], autopct='%1.1f%%', startangle=90)
    axes[2].set_title(f"Error Rate ({metrics['errors']} errors)", fontweight='bold')

    fig.suptitle(f"Real-Time Dashboard | Last Update: {metrics['last_event']}",
                  fontsize=14, fontweight='bold')
    plt.tight_layout()
    display(fig)
    plt.close(fig)

# Auto-refresh loop (run in Databricks notebook)
# for _ in range(60):
#     build_realtime_dashboard()
#     time.sleep(10)
build_realtime_dashboard()`,
  },
  {
    id: 43,
    category: 'Dashboard',
    title: 'Export Dashboard to PDF',
    desc: 'Generate PDF report with multiple charts and data tables',
    code: `import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
from datetime import datetime

df = spark.sql("SELECT * FROM monthly_report_data").toPandas()

pdf_path = '/dbfs/tmp/reports/dashboard_report.pdf'
with PdfPages(pdf_path) as pdf:
    # Page 1: Title Page
    fig = plt.figure(figsize=(11, 8.5))
    fig.text(0.5, 0.6, 'Monthly Performance Report', ha='center', fontsize=28, fontweight='bold')
    fig.text(0.5, 0.5, f'Generated: {datetime.now().strftime("%B %Y")}', ha='center', fontsize=16, color='gray')
    fig.text(0.5, 0.4, 'Databricks Analytics Team', ha='center', fontsize=14, color='#666')
    pdf.savefig(fig)
    plt.close()

    # Page 2: Revenue Trends
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    axes[0].plot(df['month'], df['revenue'], marker='o', color='#2196F3', linewidth=2)
    axes[0].set_title('Revenue Trend', fontsize=14, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    axes[1].bar(df['month'], df['profit'], color='#4CAF50')
    axes[1].set_title('Monthly Profit', fontsize=14, fontweight='bold')
    plt.tight_layout()
    pdf.savefig(fig)
    plt.close()

    # Page 3: Summary Table
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    summary = df.describe().round(2)
    table = ax.table(cellText=summary.values, rowLabels=summary.index,
                      colLabels=summary.columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)
    ax.set_title('Statistical Summary', fontsize=14, fontweight='bold', pad=30)
    pdf.savefig(fig)
    plt.close()

print(f"Report saved to {pdf_path}")`,
  },
  {
    id: 44,
    category: 'Dashboard',
    title: 'Embed IFrame Dashboard',
    desc: 'Embed external dashboards and Plotly charts via IFrame in notebooks',
    code: `# Embed external tools and visualizations in Databricks notebooks

# Method 1: Embed a Grafana dashboard
displayHTML(\"\"\"
<iframe src="https://grafana.company.com/d/dashboard-id?orgId=1&from=now-6h&to=now"
        width="100%" height="600" frameborder="0"
        style="border: 1px solid #ddd; border-radius: 8px;">
</iframe>
\"\"\")

# Method 2: Embed Plotly chart as HTML
import plotly.express as px
import plotly.io as pio

df = spark.sql("SELECT date, revenue, region FROM sales ORDER BY date").toPandas()
fig = px.line(df, x='date', y='revenue', color='region',
              title='Revenue Trends by Region')
fig.update_layout(width=1000, height=500)

# Convert to HTML and embed
html_str = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
displayHTML(html_str)

# Method 3: Embed a Power BI report
displayHTML(\"\"\"
<iframe title="Power BI Report"
        src="https://app.powerbi.com/reportEmbed?reportId=YOUR_REPORT_ID"
        width="100%" height="600" frameborder="0"
        allowFullScreen="true">
</iframe>
\"\"\")

# Method 4: Embed custom D3.js visualization
displayHTML(\"\"\"
<div id="d3-chart"></div>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
    const data = [30, 86, 168, 281, 303, 365];
    d3.select('#d3-chart').selectAll('div')
      .data(data).enter().append('div')
      .style('background', '#2196F3')
      .style('padding', '3px')
      .style('margin', '2px')
      .style('width', d => d + 'px')
      .style('color', 'white')
      .text(d => d);
</script>
\"\"\")`,
  },
  {
    id: 45,
    category: 'Geospatial',
    title: 'Folium Interactive Maps',
    desc: 'Create interactive Leaflet maps with markers, popups, and layers',
    code: `import folium
from folium.plugins import MarkerCluster, HeatMap

df = spark.sql("""
    SELECT store_name, latitude, longitude, revenue, city, state
    FROM store_locations WHERE latitude IS NOT NULL
""").toPandas()

# Create base map centered on data
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()
m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles='CartoDB positron')

# Add marker cluster
marker_cluster = MarkerCluster().add_to(m)
for _, row in df.iterrows():
    popup_html = f"""
    <div style="width: 200px;">
        <h4>{row['store_name']}</h4>
        <p><b>Location:</b> {row['city']}, {row['state']}</p>
        <p><b>Revenue:</b> \${row['revenue']:,.0f}</p>
    </div>
    """
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=folium.Popup(popup_html, max_width=250),
        tooltip=row['store_name'],
        icon=folium.Icon(color='blue' if row['revenue'] > 100000 else 'red',
                          icon='store', prefix='fa')
    ).add_to(marker_cluster)

# Add heatmap layer
heat_data = [[row['latitude'], row['longitude'], row['revenue']]
             for _, row in df.iterrows()]
HeatMap(heat_data, radius=15, blur=10, max_zoom=10).add_to(m)

# Add layer control
folium.LayerControl().add_to(m)

# Display in Databricks
displayHTML(m._repr_html_())`,
  },
  {
    id: 46,
    category: 'Geospatial',
    title: 'GeoPandas Map Visualization',
    desc: 'Thematic maps using GeoPandas with custom styling and legends',
    code: `import geopandas as gpd
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT state_code, state_name, total_sales, customer_count, avg_order_value
    FROM state_metrics
""").toPandas()

# Load US states shapefile (pre-uploaded to DBFS)
states_gdf = gpd.read_file('/dbfs/data/shapefiles/us_states.shp')
merged = states_gdf.merge(df, left_on='STUSPS', right_on='state_code', how='left')

fig, axes = plt.subplots(1, 2, figsize=(20, 8))

# Choropleth: Total Sales
merged.plot(column='total_sales', cmap='YlOrRd', linewidth=0.5,
            edgecolor='gray', legend=True, ax=axes[0],
            legend_kwds={'label': 'Total Sales ($)', 'orientation': 'horizontal', 'shrink': 0.7})
axes[0].set_title('Total Sales by State', fontsize=14, fontweight='bold')
axes[0].set_xlim(-130, -65)
axes[0].set_ylim(24, 50)
axes[0].axis('off')

# Bubble map: Customer Count
merged.plot(color='lightgray', edgecolor='gray', linewidth=0.5, ax=axes[1])
centroids = merged.geometry.centroid
scatter = axes[1].scatter(centroids.x, centroids.y,
                           s=merged['customer_count'] / 100,
                           c=merged['avg_order_value'], cmap='viridis',
                           alpha=0.6, edgecolors='black', linewidth=0.5)
fig.colorbar(scatter, ax=axes[1], label='Avg Order Value ($)', shrink=0.7)
axes[1].set_title('Customer Distribution (size) & AOV (color)', fontsize=14, fontweight='bold')
axes[1].set_xlim(-130, -65)
axes[1].set_ylim(24, 50)
axes[1].axis('off')

plt.tight_layout()
display(fig)`,
  },
  {
    id: 47,
    category: 'Geospatial',
    title: 'Choropleth with Plotly',
    desc: 'Interactive choropleth map using Plotly with hover details',
    code: `import plotly.express as px
import plotly.graph_objects as go

df = spark.sql("""
    SELECT country_code, country_name, population, gdp_per_capita,
           life_expectancy, continent
    FROM world_indicators
""").toPandas()

# World choropleth
fig = px.choropleth(df, locations='country_code', color='gdp_per_capita',
                     hover_name='country_name',
                     hover_data={'population': ':,.0f', 'life_expectancy': ':.1f',
                                 'continent': True, 'country_code': False},
                     color_continuous_scale='Plasma',
                     title='World GDP per Capita')
fig.update_layout(width=1100, height=600,
                   geo=dict(showframe=False, showcoastlines=True,
                            projection_type='natural earth'))

# Add bubble overlay for population
fig.add_trace(go.Scattergeo(
    locations=df['country_code'],
    text=df['country_name'],
    marker=dict(size=df['population'] / 1e7, color='rgba(255, 0, 0, 0.3)',
                line=dict(width=0.5, color='red')),
    name='Population',
    hovertemplate='%{text}<br>Pop: %{marker.size:.0f}M'
))

fig.update_layout(legend=dict(y=0, x=0))
fig.show()`,
  },
  {
    id: 48,
    category: 'Geospatial',
    title: 'Point Cluster Analysis',
    desc: 'Spatial clustering of point data using DBSCAN and visualization',
    code: `import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

df = spark.sql("""
    SELECT latitude, longitude, event_type, event_count
    FROM spatial_events WHERE latitude IS NOT NULL
""").toPandas()

# DBSCAN clustering on coordinates
coords = df[['latitude', 'longitude']].values
coords_scaled = StandardScaler().fit_transform(coords)
clustering = DBSCAN(eps=0.3, min_samples=5).fit(coords_scaled)
df['cluster'] = clustering.labels_

n_clusters = len(set(df['cluster'])) - (1 if -1 in df['cluster'] else 0)

fig, axes = plt.subplots(1, 2, figsize=(18, 8))

# Raw points
scatter1 = axes[0].scatter(df['longitude'], df['latitude'], c=df['event_count'],
                            cmap='hot', s=20, alpha=0.5)
fig.colorbar(scatter1, ax=axes[0], label='Event Count')
axes[0].set_title('Raw Event Locations', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')

# Clustered points
colors = plt.cm.tab20(np.linspace(0, 1, max(n_clusters + 1, 1)))
for cluster_id in sorted(df['cluster'].unique()):
    mask = df['cluster'] == cluster_id
    label = f'Cluster {cluster_id}' if cluster_id != -1 else 'Noise'
    color = 'lightgray' if cluster_id == -1 else colors[cluster_id]
    alpha = 0.2 if cluster_id == -1 else 0.7
    axes[1].scatter(df.loc[mask, 'longitude'], df.loc[mask, 'latitude'],
                     c=[color], s=30, alpha=alpha, label=label)

    if cluster_id != -1:
        centroid_lat = df.loc[mask, 'latitude'].mean()
        centroid_lon = df.loc[mask, 'longitude'].mean()
        axes[1].annotate(f'C{cluster_id}', (centroid_lon, centroid_lat),
                          fontsize=10, fontweight='bold', ha='center',
                          bbox=dict(boxstyle='round', fc='yellow', alpha=0.8))

axes[1].set_title(f'DBSCAN Clusters ({n_clusters} found)', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
axes[1].legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)

plt.tight_layout()
display(fig)`,
  },
  {
    id: 49,
    category: 'Network',
    title: 'NetworkX Graph Visualization',
    desc: 'Build and visualize network graphs from relationship data',
    code: `import networkx as nx
import matplotlib.pyplot as plt

df = spark.sql("""
    SELECT source_node, target_node, weight, relationship_type
    FROM node_relationships LIMIT 200
""").toPandas()

G = nx.from_pandas_edgelist(df, 'source_node', 'target_node',
                              edge_attr=['weight', 'relationship_type'])

# Calculate node metrics
degree = dict(G.degree())
betweenness = nx.betweenness_centrality(G)
pagerank = nx.pagerank(G)

fig, ax = plt.subplots(figsize=(16, 12))
pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

# Draw edges with width proportional to weight
edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
nx.draw_networkx_edges(G, pos, ax=ax, width=[w * 0.5 for w in edge_weights],
                        alpha=0.3, edge_color='gray')

# Draw nodes with size proportional to degree
node_sizes = [degree[n] * 100 for n in G.nodes()]
node_colors = [pagerank[n] for n in G.nodes()]
nodes = nx.draw_networkx_nodes(G, pos, ax=ax, node_size=node_sizes,
                                node_color=node_colors, cmap='YlOrRd',
                                edgecolors='black', linewidths=0.5)

# Label top nodes by betweenness
top_nodes = sorted(betweenness, key=betweenness.get, reverse=True)[:10]
labels = {n: n for n in top_nodes}
nx.draw_networkx_labels(G, pos, labels, ax=ax, font_size=8, font_weight='bold')

fig.colorbar(nodes, ax=ax, label='PageRank Score', shrink=0.7)
ax.set_title(f'Network Graph ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges)',
              fontsize=16, fontweight='bold')
ax.axis('off')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 50,
    category: 'Network',
    title: 'Force-Directed Graph with Plotly',
    desc: 'Interactive force-directed graph using Plotly for exploration',
    code: `import networkx as nx
import plotly.graph_objects as go

df = spark.sql("""
    SELECT source, target, strength, category
    FROM interaction_graph LIMIT 150
""").toPandas()

G = nx.from_pandas_edgelist(df, 'source', 'target', edge_attr=True)
pos = nx.spring_layout(G, k=1.5, iterations=80, seed=42)

# Edge traces
edge_x, edge_y = [], []
for edge in G.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

edge_trace = go.Scatter(x=edge_x, y=edge_y, mode='lines',
    line=dict(width=0.5, color='#888'), hoverinfo='none')

# Node traces
node_x = [pos[n][0] for n in G.nodes()]
node_y = [pos[n][1] for n in G.nodes()]
node_degrees = [G.degree(n) for n in G.nodes()]
node_text = [f'{n}<br>Connections: {G.degree(n)}' for n in G.nodes()]

node_trace = go.Scatter(x=node_x, y=node_y, mode='markers+text',
    text=list(G.nodes()), textposition='top center', textfont=dict(size=8),
    hovertext=node_text, hoverinfo='text',
    marker=dict(size=[d * 3 + 8 for d in node_degrees],
                color=node_degrees, colorscale='YlOrRd',
                colorbar=dict(title='Connections'),
                line=dict(width=1, color='black')))

fig = go.Figure(data=[edge_trace, node_trace])
fig.update_layout(
    title='Interactive Network Graph',
    showlegend=False, width=1000, height=800,
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    template='plotly_white'
)
fig.show()`,
  },
  {
    id: 51,
    category: 'Network',
    title: 'Community Detection Visualization',
    desc: 'Detect and visualize communities in network graphs using modularity',
    code: `import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.community import greedy_modularity_communities
import numpy as np

df = spark.sql("""
    SELECT user_a, user_b, interaction_count
    FROM social_interactions WHERE interaction_count > 3
    LIMIT 300
""").toPandas()

G = nx.from_pandas_edgelist(df, 'user_a', 'user_b', edge_attr='interaction_count')

# Community detection using greedy modularity
communities = list(greedy_modularity_communities(G))

# Assign community labels
node_community = {}
for i, comm in enumerate(communities):
    for node in comm:
        node_community[node] = i

n_communities = len(communities)
colors = plt.cm.tab20(np.linspace(0, 1, max(n_communities, 1)))
node_colors = [colors[node_community[n]] for n in G.nodes()]

fig, axes = plt.subplots(1, 2, figsize=(20, 9))

# Community layout
pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
nx.draw_networkx_edges(G, pos, ax=axes[0], alpha=0.1, width=0.5)
nx.draw_networkx_nodes(G, pos, ax=axes[0], node_color=node_colors,
                        node_size=80, edgecolors='black', linewidths=0.3)
axes[0].set_title(f'Community Structure ({n_communities} communities)',
                   fontsize=14, fontweight='bold')
axes[0].axis('off')

# Community size distribution
comm_sizes = [len(c) for c in communities]
comm_labels = [f'C{i}' for i in range(n_communities)]
bars = axes[1].bar(comm_labels, comm_sizes,
                    color=[colors[i] for i in range(n_communities)],
                    edgecolor='black', linewidth=0.5)
axes[1].set_title('Community Size Distribution', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Community ID')
axes[1].set_ylabel('Number of Nodes')
for bar, size in zip(bars, comm_sizes):
    axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                  str(size), ha='center', fontweight='bold', fontsize=9)

modularity = nx.community.modularity(G, communities)
fig.suptitle(f'Network Community Analysis (Modularity: {modularity:.3f})',
              fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 52,
    category: 'Network',
    title: 'Hierarchy / Tree Visualization',
    desc: 'Visualize hierarchical tree structures like org charts or taxonomies',
    code: `import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

df = spark.sql("""
    SELECT parent_id, child_id, node_name, level, department
    FROM org_hierarchy
""").toPandas()

G = nx.DiGraph()
for _, row in df.iterrows():
    G.add_node(row['child_id'], name=row['node_name'], level=row['level'],
               dept=row['department'])
    if pd.notna(row['parent_id']):
        G.add_edge(row['parent_id'], row['child_id'])

# Find root node
roots = [n for n, d in G.in_degree() if d == 0]

def hierarchy_pos(G, root, width=1.0):
    pos = {}
    def _place(node, left, right, depth=0):
        pos[node] = ((left + right) / 2, -depth)
        children = list(G.successors(node))
        if children:
            step = (right - left) / len(children)
            for i, child in enumerate(children):
                _place(child, left + i * step, left + (i + 1) * step, depth + 1)
    _place(root, 0, width)
    return pos

fig, ax = plt.subplots(figsize=(20, 12))
pos = hierarchy_pos(G, roots[0], width=10)

# Color by department
depts = list(set(nx.get_node_attributes(G, 'dept').values()))
dept_colors = {d: plt.cm.Set3(i / max(len(depts), 1)) for i, d in enumerate(depts)}
node_colors = [dept_colors.get(G.nodes[n].get('dept', ''), 'lightgray') for n in G.nodes()]

nx.draw_networkx_edges(G, pos, ax=ax, arrows=True, arrowstyle='-|>',
                        edge_color='gray', width=1.5, alpha=0.7)
nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors,
                        node_size=800, edgecolors='black', linewidths=1)

labels = {n: G.nodes[n].get('name', str(n)) for n in G.nodes()}
nx.draw_networkx_labels(G, pos, labels, ax=ax, font_size=7, font_weight='bold')

# Legend
for dept, color in dept_colors.items():
    ax.scatter([], [], c=[color], s=100, label=dept, edgecolors='black')
ax.legend(title='Department', loc='upper right', fontsize=9)

ax.set_title('Organization Hierarchy', fontsize=18, fontweight='bold')
ax.axis('off')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 53,
    category: 'Statistical',
    title: 'QQ Plot - Normality Test',
    desc: 'Quantile-Quantile plot to assess distribution normality',
    code: `import matplotlib.pyplot as plt
from scipy import stats
import numpy as np

df = spark.sql("""
    SELECT feature_1, feature_2, feature_3, feature_4
    FROM model_features ORDER BY RAND() LIMIT 5000
""").toPandas()

features = ['feature_1', 'feature_2', 'feature_3', 'feature_4']
fig, axes = plt.subplots(2, 2, figsize=(14, 12))
axes = axes.flatten()

for i, feat in enumerate(features):
    ax = axes[i]
    data = df[feat].dropna()

    # QQ plot
    (osm, osr), (slope, intercept, r) = stats.probplot(data, dist='norm', plot=ax)
    ax.get_lines()[0].set_markerfacecolor('#2196F3')
    ax.get_lines()[0].set_markeredgecolor('#1565C0')
    ax.get_lines()[0].set_markersize(4)
    ax.get_lines()[1].set_color('#F44336')
    ax.get_lines()[1].set_linewidth(2)

    # Shapiro-Wilk test
    sample = data.sample(min(5000, len(data)))
    stat_sw, p_sw = stats.shapiro(sample)

    normality = 'Normal' if p_sw > 0.05 else 'Non-Normal'
    color = '#4CAF50' if p_sw > 0.05 else '#F44336'

    ax.set_title(f'{feat} (R2={r**2:.4f})', fontsize=12, fontweight='bold')
    ax.text(0.05, 0.92, f'Shapiro-Wilk p={p_sw:.4f}\\n{normality}',
            transform=ax.transAxes, fontsize=9, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor=color, alpha=0.2))
    ax.grid(True, alpha=0.3)

fig.suptitle('QQ Plots - Normality Assessment', fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
  {
    id: 54,
    category: 'Statistical',
    title: 'Residual Analysis Plot',
    desc: 'Comprehensive residual diagnostics for regression model validation',
    code: `import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression

df = spark.sql("SELECT * FROM regression_data").toPandas()
X = df[['feature_1', 'feature_2', 'feature_3']].values
y = df['target'].values

model = LinearRegression().fit(X, y)
y_pred = model.predict(X)
residuals = y - y_pred
std_residuals = (residuals - residuals.mean()) / residuals.std()

fig, axes = plt.subplots(2, 2, figsize=(14, 12))

# 1. Residuals vs Fitted
axes[0, 0].scatter(y_pred, residuals, alpha=0.4, s=20, color='#2196F3')
axes[0, 0].axhline(y=0, color='red', linestyle='--', linewidth=2)
axes[0, 0].set_xlabel('Fitted Values')
axes[0, 0].set_ylabel('Residuals')
axes[0, 0].set_title('Residuals vs Fitted', fontweight='bold')
z = np.polyfit(y_pred, residuals, 3)
p = np.poly1d(z)
x_sorted = np.sort(y_pred)
axes[0, 0].plot(x_sorted, p(x_sorted), color='orange', linewidth=2)

# 2. QQ Plot of residuals
stats.probplot(std_residuals, dist='norm', plot=axes[0, 1])
axes[0, 1].set_title('Normal Q-Q Plot', fontweight='bold')
axes[0, 1].get_lines()[0].set_markerfacecolor('#2196F3')
axes[0, 1].get_lines()[1].set_color('#F44336')

# 3. Scale-Location
axes[1, 0].scatter(y_pred, np.sqrt(np.abs(std_residuals)), alpha=0.4, s=20, color='#4CAF50')
axes[1, 0].set_xlabel('Fitted Values')
axes[1, 0].set_ylabel('Sqrt |Standardized Residuals|')
axes[1, 0].set_title('Scale-Location (Homoscedasticity)', fontweight='bold')

# 4. Residuals histogram
axes[1, 1].hist(std_residuals, bins=40, density=True, alpha=0.7, color='#FF9800', edgecolor='white')
x_range = np.linspace(-4, 4, 100)
axes[1, 1].plot(x_range, stats.norm.pdf(x_range), 'r-', linewidth=2, label='Normal')
axes[1, 1].set_title('Residual Distribution', fontweight='bold')
axes[1, 1].legend()

# Durbin-Watson test
from statsmodels.stats.stattools import durbin_watson
dw = durbin_watson(residuals)
fig.suptitle(f'Residual Diagnostics (R2={model.score(X, y):.4f}, DW={dw:.3f})',
              fontsize=16, fontweight='bold')

for ax in axes.flatten():
    ax.grid(True, alpha=0.3)
plt.tight_layout()
display(fig)`,
  },
  {
    id: 55,
    category: 'Statistical',
    title: 'ROC Curve & AUC Analysis',
    desc: 'ROC curve with AUC, precision-recall curve, and threshold analysis',
    code: `import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import (roc_curve, auc, precision_recall_curve,
                              average_precision_score, confusion_matrix)

df = spark.sql("""
    SELECT y_true, y_prob_class1 FROM model_predictions
""").toPandas()
y_true = df['y_true'].values
y_scores = df['y_prob_class1'].values

fig, axes = plt.subplots(2, 2, figsize=(14, 12))

# 1. ROC Curve
fpr, tpr, thresholds_roc = roc_curve(y_true, y_scores)
roc_auc = auc(fpr, tpr)
axes[0, 0].plot(fpr, tpr, color='#2196F3', linewidth=2, label=f'ROC (AUC = {roc_auc:.4f})')
axes[0, 0].plot([0, 1], [0, 1], 'k--', alpha=0.5, label='Random')
axes[0, 0].fill_between(fpr, tpr, alpha=0.1, color='#2196F3')
# Optimal threshold (Youden J)
j_scores = tpr - fpr
optimal_idx = np.argmax(j_scores)
axes[0, 0].scatter(fpr[optimal_idx], tpr[optimal_idx], c='red', s=100, zorder=5,
                    label=f'Optimal (t={thresholds_roc[optimal_idx]:.3f})')
axes[0, 0].set_title('ROC Curve', fontweight='bold')
axes[0, 0].set_xlabel('False Positive Rate')
axes[0, 0].set_ylabel('True Positive Rate')
axes[0, 0].legend(loc='lower right')

# 2. Precision-Recall Curve
precision, recall, thresholds_pr = precision_recall_curve(y_true, y_scores)
avg_prec = average_precision_score(y_true, y_scores)
axes[0, 1].plot(recall, precision, color='#4CAF50', linewidth=2,
                 label=f'PR (AP = {avg_prec:.4f})')
axes[0, 1].fill_between(recall, precision, alpha=0.1, color='#4CAF50')
axes[0, 1].set_title('Precision-Recall Curve', fontweight='bold')
axes[0, 1].set_xlabel('Recall')
axes[0, 1].set_ylabel('Precision')
axes[0, 1].legend()

# 3. Threshold vs Metrics
thresholds_plot = np.linspace(0, 1, 100)
tpr_interp = np.interp(thresholds_plot, thresholds_roc[::-1], tpr[::-1])
fpr_interp = np.interp(thresholds_plot, thresholds_roc[::-1], fpr[::-1])
axes[1, 0].plot(thresholds_plot, tpr_interp, label='TPR (Sensitivity)',
                 color='#2196F3', linewidth=2)
axes[1, 0].plot(thresholds_plot, 1 - fpr_interp, label='TNR (Specificity)',
                 color='#FF9800', linewidth=2)
axes[1, 0].axvline(x=thresholds_roc[optimal_idx], color='red', linestyle='--',
                    label=f'Optimal: {thresholds_roc[optimal_idx]:.3f}')
axes[1, 0].set_title('Threshold Analysis', fontweight='bold')
axes[1, 0].set_xlabel('Threshold')
axes[1, 0].set_ylabel('Rate')
axes[1, 0].legend()

# 4. Confusion Matrix at optimal threshold
y_pred_opt = (y_scores >= thresholds_roc[optimal_idx]).astype(int)
cm = confusion_matrix(y_true, y_pred_opt)
im = axes[1, 1].imshow(cm, cmap='Blues')
for i in range(2):
    for j in range(2):
        axes[1, 1].text(j, i, f'{cm[i, j]:,}', ha='center', va='center',
                         fontsize=16, fontweight='bold',
                         color='white' if cm[i, j] > cm.max()/2 else 'black')
axes[1, 1].set_title(f'Confusion Matrix (t={thresholds_roc[optimal_idx]:.3f})', fontweight='bold')
axes[1, 1].set_xlabel('Predicted')
axes[1, 1].set_ylabel('Actual')
axes[1, 1].set_xticks([0, 1])
axes[1, 1].set_yticks([0, 1])
axes[1, 1].set_xticklabels(['Negative', 'Positive'])
axes[1, 1].set_yticklabels(['Negative', 'Positive'])

for ax in axes.flatten():
    ax.grid(True, alpha=0.3)

fig.suptitle(f'Model Evaluation (AUC={roc_auc:.4f}, AP={avg_prec:.4f})',
              fontsize=16, fontweight='bold')
plt.tight_layout()
display(fig)`,
  },
];

const categories = [...new Set(visualizationScenarios.map((s) => s.category))];

function Visualization() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = visualizationScenarios.filter((s) => {
    const matchCategory = selectedCategory === 'All' || s.category === selectedCategory;
    const matchSearch =
      s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCategory && matchSearch;
  });

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Visualization Scenarios</h1>
          <p>{visualizationScenarios.length} PySpark visualization patterns for Databricks</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '300px' }}
          />
          <select
            className="form-input"
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value)}
            style={{ maxWidth: '200px' }}
          >
            <option value="All">All Categories ({visualizationScenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({visualizationScenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {visualizationScenarios.length}
          </span>
        </div>
      </div>

      <div className="scenarios-list">
        {filtered.map((scenario) => (
          <div key={scenario.id} className="card scenario-card" style={{ marginBottom: '0.75rem' }}>
            <div
              className="scenario-header"
              onClick={() => setExpandedId(expandedId === scenario.id ? null : scenario.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '0.25rem',
                  }}
                >
                  <span className="badge pending">{scenario.category}</span>
                  <strong>
                    #{scenario.id} — {scenario.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {scenario.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === scenario.id ? '\u25BC' : '\u25B6'}
              </span>
            </div>
            {expandedId === scenario.id && (
              <div className="code-block" style={{ marginTop: '1rem' }}>
                {scenario.code}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default Visualization;
