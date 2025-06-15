import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import emoji

# Database path
DB_PATH = r"./money_manager.db"

def execute_query(sql_script: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql_script, conn)

def clean_emoji(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Removes emojis from a specified column in a DataFrame.
    """
    df[column_name] = df[column_name].apply(lambda x: emoji.replace_emoji(x, replace=""))
    return df

def get_monthly_summary() -> pd.DataFrame:
    """Fetches the total income and expenses for each month."""
    return execute_query('''
        SELECT 
            strftime('%Y-%m', WDATE) AS Month,
            SUM(CASE WHEN DO_TYPE = 1 THEN ZMONEY ELSE 0 END) AS TotalExpenses,
            SUM(CASE WHEN DO_TYPE = 0 THEN ZMONEY ELSE 0 END) AS TotalIncome
        FROM INOUTCOME
        GROUP BY Month
        ORDER BY Month;
    ''')

def get_monthly_net_revenue() -> pd.DataFrame:
    """Calculates the net revenue for each month."""
    return execute_query('''
        SELECT 
            strftime('%Y-%m', WDATE) AS Month,
            SUM(CASE WHEN DO_TYPE = 0 THEN ZMONEY ELSE 0 END) -
            SUM(CASE WHEN DO_TYPE = 1 THEN ZMONEY ELSE 0 END) AS NetRevenue
        FROM INOUTCOME
        GROUP BY Month
        ORDER BY Month;
    ''')

def get_revenue_analysis_data() -> pd.DataFrame:
    """Gathers data for monthly revenue analysis, including income, expenses, and net revenue."""
    return execute_query('''
        SELECT 
            strftime('%m-%Y', WDATE) AS Month_Year, 
            SUM(CASE WHEN DO_TYPE = 0 THEN ZMONEY ELSE 0 END) AS TotalIncome,
            SUM(CASE WHEN DO_TYPE = 1 THEN ZMONEY ELSE 0 END) AS TotalExpenses,
            SUM(CASE WHEN DO_TYPE = 0 THEN ZMONEY ELSE 0 END) - SUM(CASE WHEN DO_TYPE = 1 THEN ZMONEY ELSE 0 END) AS Revenue
        FROM INOUTCOME
        GROUP BY Month_Year
        ORDER BY strftime('%Y', WDATE), strftime('%m', WDATE);
    ''')

def get_average_monthly_figure(transaction_type: int) -> pd.DataFrame:
    """
    Calculates the average monthly income or expense.
    """
    entity = "Spending" if transaction_type == 1 else "Income"
    return execute_query(f'''
        SELECT ROUND(AVG(monthly_total), 2) AS AverageMonthly{entity}
        FROM (
            SELECT strftime('%Y-%m', WDATE) AS Month, SUM(ZMONEY) AS monthly_total
            FROM INOUTCOME
            WHERE DO_TYPE = {transaction_type}
            GROUP BY Month
        );
    ''')

def get_summary_by_category(transaction_type: int, by_main_category: bool = False) -> pd.DataFrame:
    """
    Summarizes financial data by category or main category.
    """
    category_col = "COALESCE(mc.NAME, c.NAME) AS Category" if by_main_category else "c.NAME AS Category"
    entity = "TotalSpent" if transaction_type == 1 else "TotalIncome"
    join_clause = "LEFT JOIN ZCATEGORY mc ON c.pUid = mc.uid" if by_main_category else ""
    group_by_col = "COALESCE(mc.NAME, c.NAME)" if by_main_category else "c.NAME"

    query = f'''
        SELECT 
            {category_col},
            SUM(i.ZMONEY) AS {entity}
        FROM INOUTCOME i
        JOIN ZCATEGORY c ON i.ctgUid = c.uid
        {join_clause}
        WHERE i.DO_TYPE = {transaction_type}
        GROUP BY {group_by_col}
        ORDER BY {entity} DESC;
    '''
    df = execute_query(query)
    # The `Category` column in get_summary_by_category will be either 'Category' or 'MainCategory' depending on `by_main_category`
    # We should ensure we clean the correct column name.
    if by_main_category:
        return clean_emoji(df, "Category") # The alias is always 'Category' in this function's result.
    else:
        return clean_emoji(df, "Category")
    
def get_monthly_summary_by_category(transaction_type: int, by_main_category: bool = False) -> pd.DataFrame:
    """
    Creates a pivot table of monthly financial data by category.
    """
    category_col_alias = "MainCategory" if by_main_category else "Category"
    category_col = f"COALESCE(mc.NAME, c.NAME) AS {category_col_alias}" if by_main_category else f"c.NAME AS {category_col_alias}"
    entity = "TotalExpense" if transaction_type == 1 else "TotalIncome"
    join_clause = "LEFT JOIN ZCATEGORY mc ON c.pUid = mc.uid" if by_main_category else ""

    query = f'''
        SELECT 
            strftime('%Y-%m', i.WDATE) AS Month,
            {category_col},
            ROUND(SUM(i.ZMONEY), 2) AS {entity}
        FROM INOUTCOME i
        JOIN ZCATEGORY c ON i.ctgUid = c.uid
        {join_clause}
        WHERE i.DO_TYPE = {transaction_type}
        GROUP BY Month, {category_col_alias}
        ORDER BY Month, {entity} DESC;
    '''
    
    monthly_data = execute_query(query)
    clean_emoji(monthly_data, category_col_alias)

    return monthly_data.pivot_table(
        index='Month',
        columns=category_col_alias,
        values=entity,
        fill_value=0
    ).round(2)


def get_average_expense_by_main_category() -> pd.DataFrame:
    """Calculates the average monthly expense for each main category."""
    query = '''
        SELECT 
            MainCategory,
            ROUND(AVG(MonthlyTotal), 2) AS AvgMonthlyExpense
        FROM (
            SELECT 
                COALESCE(mc.NAME, c.NAME) AS MainCategory,
                strftime('%Y-%m', i.WDATE) AS YearMonth,
                SUM(i.ZMONEY) AS MonthlyTotal
            FROM INOUTCOME i
            JOIN ZCATEGORY c ON i.ctgUid = c.uid
            LEFT JOIN ZCATEGORY mc ON c.pUid = mc.uid
            WHERE i.DO_TYPE = 1
            GROUP BY MainCategory, YearMonth
        ) AS MonthlySums
        GROUP BY MainCategory
        ORDER BY AvgMonthlyExpense DESC;
    '''
    df = execute_query(query)
    return clean_emoji(df, "MainCategory")


def get_summary_by_payment_method(transaction_type: int) -> pd.DataFrame:
    """
    Summarizes transactions by payment method.
    """
    entity = "TotalSpent" if transaction_type == 1 else "TotalReceived"
    return execute_query(f'''
        SELECT 
            a.NIC_NAME AS PaymentMethod,
            SUM(i.ZMONEY) AS {entity}
        FROM INOUTCOME i
        JOIN ASSETS a ON i.assetUid = a.uid
        WHERE i.DO_TYPE = {transaction_type}
        GROUP BY a.NIC_NAME
        ORDER BY {entity} DESC;
    ''')


def get_month_with_highest_expense() -> pd.DataFrame:
    """Identifies the month with the highest total expenses."""
    return execute_query('''
        SELECT strftime('%Y-%m', WDATE) AS Month, SUM(ZMONEY) AS Total
        FROM INOUTCOME
        WHERE DO_TYPE = 1
        GROUP BY Month
        ORDER BY Total DESC
        LIMIT 1;
    ''')



def get_monthly_expense_distribution() -> pd.DataFrame:
    """Fetches the distribution of expenses by category for each month."""
    query = '''
        SELECT 
            strftime('%Y-%m', i.WDATE) AS YearMonth,
            c.NAME AS Category,
            SUM(i.ZMONEY) AS TotalSpent
        FROM INOUTCOME i
        JOIN ZCATEGORY c ON i.ctgUid = c.uid
        WHERE i.DO_TYPE = 1
        GROUP BY YearMonth, Category
        ORDER BY YearMonth, TotalSpent DESC;
    '''
    df = execute_query(query)
    df = clean_emoji(df, "Category") 
    
    return df

# --- Plotting Functions ---

def plot_revenue_analysis(df: pd.DataFrame):
    """Plots monthly income, expenses, and revenue."""
    plt.figure(figsize=(12, 6))
    plt.plot(df["Month_Year"], df["TotalIncome"], label="Income", marker="o", color="green")
    plt.plot(df["Month_Year"], df["TotalExpenses"], label="Expenses", marker="o", color="red")
    plt.plot(df["Month_Year"], df["Revenue"], label="Revenue", marker="o", color="blue")
    plt.xlabel("Month-Year")
    plt.ylabel("Amount (€)")
    plt.title("Monthly Revenue Analysis")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_expenses_by_main_category(df: pd.DataFrame):
    """Plots a bar chart of total expenses by main category."""
    df.plot(
        kind='bar',
        x='Category',
        y='TotalSpent',
        figsize=(10, 6),
        legend=False,
        color='slateblue'
    )
    plt.title("Total Expenses by Main Category")
    plt.ylabel("Total Expenses (€)")
    plt.xlabel("Main Category")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()


def plot_monthly_trends_by_category(df_pivot: pd.DataFrame, title: str):
    """
    Plots monthly trends for different categories from a pivot table.
    """
    plt.figure(figsize=(14, 7))
    for category in df_pivot.columns:
        plt.plot(df_pivot.index, df_pivot[category], marker='o', label=category)
    plt.title(title)
    plt.xlabel("Month")
    plt.ylabel("Amount (€)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.tight_layout()
    plt.show()


def plot_monthly_expense_distribution_pie(df: pd.DataFrame):
    """
    Generates a pie chart for expense distribution for each month.
    """
    for month in df["YearMonth"].unique():
        data = df[df["YearMonth"] == month].copy() 
        
        # Filter out negative expenses as pie charts cannot represent them
        data = data[data["TotalSpent"] >= 0] 
        
        # Skip plotting if there's no positive data for the month
        if data.empty or data["TotalSpent"].sum() == 0:
            print(f"No positive expenses to plot for {month}. Skipping pie chart.")
            continue

        plt.figure(figsize=(8, 8))
        plt.pie(data["TotalSpent"], labels=data["Category"], autopct='%1.1f%%', startangle=140)
        plt.title(f"Expense Distribution by Category for {month}")
        plt.tight_layout()
        plt.show()

def plot_average_monthly_expense_pie(df: pd.DataFrame):
    """
    Generates a pie chart for the average monthly expense by main category.
    """
    labels = df["MainCategory"]
    sizes = df["AvgMonthlyExpense"]
    
    colors = plt.cm.viridis_r([i/float(len(labels)) for i in range(len(labels))])

    fig, ax = plt.subplots(figsize=(12, 8))
    wedges, texts, autotexts = ax.pie(
        sizes,
        autopct='%1.1f%%',
        startangle=140,
        colors=colors,
        pctdistance=0.85
    )
    
    ax.axis("equal")
    ax.legend(wedges, labels, title="Main Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    plt.setp(autotexts, size=8, weight="bold")
    ax.set_title("Average Monthly Expense by Main Category")
    plt.show()


if __name__ == "__main__":
    # --- Data Retrieval and Display ---
    monthly_summary = get_monthly_summary()
    print("--- Monthly Income and Expenses ---\n", monthly_summary, "\n")
    
    monthly_net_revenue = get_monthly_net_revenue()
    print("--- Monthly Net Revenue ---\n", monthly_net_revenue, "\n")
    
    avg_monthly_expense = get_average_monthly_figure(transaction_type=1)
    print("--- Average Monthly Expense ---\n", avg_monthly_expense, "\n")

    avg_monthly_income = get_average_monthly_figure(transaction_type=0)
    print("--- Average Monthly Income ---\n", avg_monthly_income, "\n")

    expense_by_category = get_summary_by_category(transaction_type=1)
    print("--- Expenses by Sub-Category ---\n", expense_by_category, "\n")
    
    expense_by_main_category = get_summary_by_category(transaction_type=1, by_main_category=True)
    print("--- Expenses by Main Category ---\n", expense_by_main_category, "\n")

    income_by_category = get_summary_by_category(transaction_type=0, by_main_category=True)
    print("--- Income by Main Category ---\n", income_by_category, "\n")

    monthly_expense_pivot = get_monthly_summary_by_category(transaction_type=1)
    print("--- Monthly Expenses by Sub-Category (Pivot) ---\n", monthly_expense_pivot, "\n")
    
    monthly_main_expense_pivot = get_monthly_summary_by_category(transaction_type=1, by_main_category=True)
    print("--- Monthly Expenses by Main Category (Pivot) ---\n", monthly_main_expense_pivot, "\n")

    monthly_main_income_pivot = get_monthly_summary_by_category(transaction_type=0, by_main_category=True)
    print("--- Monthly Income by Main Category (Pivot) ---\n", monthly_main_income_pivot, "\n")
    
    avg_expense_main_category = get_average_expense_by_main_category()
    print("--- Average Monthly Expense by Main Category ---\n", avg_expense_main_category, "\n")

    expenses_by_payment_method = get_summary_by_payment_method(transaction_type=1)
    print("--- Expenses by Payment Method ---\n", expenses_by_payment_method, "\n")

    income_by_payment_method = get_summary_by_payment_method(transaction_type=0)
    print("--- Income by Payment Method ---\n", income_by_payment_method, "\n")

    month_highest_expense = get_month_with_highest_expense()
    print("--- Month with Highest Expenses ---\n", month_highest_expense, "\n")

    # --- Visualizations ---
    revenue_data = get_revenue_analysis_data()
    plot_revenue_analysis(revenue_data)

    plot_expenses_by_main_category(expense_by_main_category)
    
    plot_monthly_trends_by_category(monthly_main_expense_pivot, "Monthly Expense Trends by Main Category")
    
    plot_monthly_trends_by_category(monthly_main_income_pivot, "Monthly Income Trends by Main Category")
    
    monthly_expense_dist = get_monthly_expense_distribution()
    plot_monthly_expense_distribution_pie(monthly_expense_dist)

    plot_average_monthly_expense_pie(avg_expense_main_category)