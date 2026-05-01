import streamlit as st

pg = st.navigation({
    "account": [
        st.Page("app/1_Account/dashboard.py", title="Dashboard", default=True),
        st.Page("app/1_Account/serviceKey.py", title="Service Key"),
    ],
    "data": [
        st.Page("app/2_DATA/DART.py", title="DART 전자공시"),
        st.Page("app/2_DATA/KIS.py", title="KIS"),
        st.Page("app/2_DATA/NFR.py", title="네이버금융 리서치"),
    ],
    



}, position="sidebar", expanded=True)

pg.run()