import streamlit as st

pg = st.navigation({
    "Home": [
        st.Page("app/0_Home/1_Overview.py", title="Overview", default=True),
        st.Page("app/0_Home/2_About_me.py", title="About me"),
        st.Page("app/0_Home/3_References.py", title="References")
    ],
    "Data Team": [
        st.Page("app/1_Data_Team/1_Market_data.py", title="Market Data"),
        st.Page("app/1_Data_Team/2_Trading_Trends.py", title="Trading Trends"),
        st.Page("app/1_Data_Team/3_Brokerage_Recommendations.py", title="Brokerage Recommendations"),
        st.Page("app/1_Data_Team/4_Retail_Investors_data.py", title="Retail Investors Data"),
        st.Page("app/1_Data_Team/5_News_data.py", title="News Data"),
    ],
    "Research Team": [
        st.Page("app/2_Research_Team/1_Sentiment_Analysis.py", title="Sentiment Analysis"),
        st.Page("app/2_Research_Team/2_Technical_Indicators.py", title="Technical Indicators"),
    ],
    "Portfolio Management Team": [

    ],



}, position="sidebar", expanded=True)

pg.run()
