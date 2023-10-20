import json
import pandas as pd
import streamlit as st
from PIL import Image
import os
import time

from functions import *
from dotenv import load_dotenv

# Initialize session state
if 'login_status' not in st.session_state:
    st.session_state.login_status = False

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Set page width
st.set_page_config(layout="wide")

# Simple Auth
valid_user = {os.environ.get("STREAMLIT_USERNAME"): os.environ.get("STREAMLIT_PASSWORD")}
st.sidebar.header("User Login")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")
st.sidebar.info("Login required for use")

if st.sidebar.button("Login"):
    if username and password:
        if username in valid_user and valid_user[username] == password:
            st.session_state.login_status = True
            st.sidebar.success("Logged in successfully")
        else:
            st.sidebar.error("Invalid username or password")

# Header
st.markdown("<h1 style='text-align: center; color: #FFA500;'>AWS Data Pipeline Project: Showcase Frontend</h1>", unsafe_allow_html=True)

# Intro Text
centered_text = '''
    <div style="text-align: center; font-size: 18px;">
        AWS 서비스를 이용한 Data Engineering 프로젝트 샘플 앱<br>
    </div>
'''
st.markdown(centered_text, unsafe_allow_html=True)


def main_app():
    MainTab, ArchiTab = st.tabs(["Main", "Architecture"])

    with MainTab:
        
        # Explanation 
        st.markdown(
            """
            <style>
                .container {
                    background-color: rgba(211, 211, 211, 0.5);
                    padding: 20px;
                    border-radius: 15px;
                }
                .header {
                    font-weight: bold;
                    font-size: 24px;
                }
                .sub-header {
                    font-weight: bold
                }
                .info-text {
                    padding-left: 20px;
                }
            </style>
            <div class="container">
                <p class="header">개요</p>
                <p class="info-text"><i>참고: 전체 구성과 소스코드 링크 확인을 위해서는 상단 아키텍쳐 탭 참고</i></p>
                <p class="sub-header">- 가상의 e-커머스 화장품 회사의 이용자등록, 매출, 재고 데이터와 사용자 로그 정보를 데이터로 사용헸습니다:</p>
                <ul class="info-text">
                    <li>비즈니스 데이터: users, sales, inventory</li>
                    <li>사용자 로그 데이터: IP, location, 검색 결과 등</li>
                </ul>
                <p class="sub-header">- 데이터 엔지니어링 프로젝트지만 간략하게 파이프라인 데이터를 이용하여 해당 쇼케이스를 제작하였습니다:</p>
                <ul class="info-text">
                    <li>모든 파이프라인은 AWS 서비스를 이용하여 구축되었습니다</li>
                    <li>파이프라인은 Live 상태이며 가상 데이터를 조금씩 적재 중입니다</li>
                </ul>
            </div>
            """, 
            unsafe_allow_html=True
        )

        st.markdown('---')
        
        # Elastic
        st.image(Image.open('app/images/elastic.png'), width=200)
        st.markdown(" ## :blue[Elastic Stack Pipeline]")
        elastic1, elastic2 = st.columns(2)
        with elastic1:
            elastic_text = """
                ***Logstash***를 이용하여 ***MSK*** 스트림으로 부터 가상의 사용자 로그 데이터를 수집하여,  
                ***Elasticsearch***와 ***S3***에 동시 적재하는 파이프라인을 구축하였습니다. \n
                Elastic Stack API를 연동하여 아래와 같이 제품과 브라우저를 선택하여 검색한 사용자의 위치를 지도에 표시하는 간략한 기능입니다. \n
                :red[10 ~ 20 초 간격으로 데이터가 ***MSK***로 프로듀싱 되도록 설계되었기에 시간이 지나면서 동일 검색으로도 결과가 달라질 수 있습니다.] \n
            """
            st.markdown(elastic_text)
            search_keyword = st.selectbox('이용자가 검색한 상품 선택:', ["Lipstick", "Foundation", "Mascara", "Eyeliner", "Nail Polish", "BB"])
            user_browser = st.selectbox('이용자가 접속한 브라우저 선택:', ["Chrome", "Firefox", "Safari", "Opera"])

            if st.button("Run Search"):
                query = construct_query()
                raw_messages = execute_query(query)
                            
                filtered_messages = []
                for msg in raw_messages:
                    parsed_msg = json.loads(msg)
                    if search_keyword.lower() in parsed_msg.get("search_keywords", "").lower() and \
                        any(browser in parsed_msg.get("user_agent", "") for browser in [user_browser, user_browser.lower()]):
                        filtered_messages.append(parsed_msg)
                            
                df = pd.DataFrame(filtered_messages)
                st.session_state.elastic_df = df 
                        
            with elastic2:
                if 'elastic_df' in st.session_state and st.session_state.elastic_df is not None:
                    map_df = st.session_state.elastic_df['location'].apply(pd.Series)
                    map_df = map_df.rename(columns={0: 'lat', 1: 'lon'})
                    map_df['lat'] = pd.to_numeric(map_df['lat'], errors='coerce')
                    map_df['lon'] = pd.to_numeric(map_df['lon'], errors='coerce')
                    map_df = map_df.dropna(subset=['lat', 'lon'])

                    st.markdown("### :blue[User Locations & Search Keyword Map]")
                    st.map(map_df)

        st.markdown("""
            <hr style='margin-top: 10px; margin-bottom: 10px; border: 1px solid grey;'>
        """, unsafe_allow_html=True)
        st.markdown(" ### :blue[Elasticsearch Keyword Search with NLP]")
        raw_search_text = """
            간략하게 ***SpaCY*** NLP를 적용하여 ***Elasticsearch***의 검색 기능을 확장하였습니다. 키워드 검색을 통해 검색된 결과를 표시합니다. \n
            :red[**예시:** *'Chrome, Lipstick, Stockholm 2023-09`* 검색 시 2023-09에 스톡홀름에서 크롬 브라우저를 사용하여 립스틱을 검색한 사용자 로그를 표시합니다.]\n
            - 파이썬 *Faker* 라이브러리를 이용하여 가상의 데이터를 생성하여 파이프라인을 구축하여 제한적임을 참고 부탁드리겠습니다. 
            - 비슷한 맥락으로 한국어를 위한 Elasticsearch의 ***Nori*** 플러그인도 적용은 하였으나 한글 데이터는 사용하지 않았습니다. 
        """
        st.markdown(raw_search_text)
        user_query = st.text_input("키워드 체인으로 검색:")

        if user_query:
            results = search(user_query)
            
            if results['total']['value'] > 0:
                st.markdown("#### :blue[Search Results]")

                all_cards = ''
                
                for hit in results['hits']:
                    hit_source = hit["_source"]
                    message = json.loads(hit_source["message"]) 
                    
                    card_content = '<div style="border: 2px solid gray; padding: 16px; margin: 16px; border-radius: 8px;">'
                    
                    for key, value in message.items():
                        card_content += f'<p><strong style="color:teal;">{key}:</strong> <span style="color:orange;">{value}</span></p>'
                    
                    card_content += '</div>'
                    
                    all_cards += card_content  
                    
                st.markdown(f'<div style="max-height:400px; overflow:auto;">{all_cards}</div>', unsafe_allow_html=True)
                
            else:
                st.write("No results found.")

        st.write('---')
        st.write('---')

        if 'iceberg_df' not in st.session_state:
            st.session_state.iceberg_df = None

        st.image(Image.open('app/images/iceberg.png'), width=200)
        st.markdown("## :violet[Iceberg Warehouse Pipeline]")
        elastic_text = """
            ***MSK*** 스트림으로부터 가상의 데이터를 수집하는 ***Spark Structured Stream***을 구축하여 ***S3***에 적재하였습니다. \n
            이후 해당 데이터를 ***S3***에서 ***Apache Iceberg***형태의 웨어하우스 테이블로 벼형하는 배치 파이프라인을 ***EMR Serverless*** 스파크SQL로 구축했으며,  
            ***Glue*** & ***Athena***를 이용하여 쿼리가 가능한 가상 서비스를 구축하였습니다. \n
            배치 파이프라인은 ***Airflow***를 이용하여 스케줄링하였으며 1일 단위로 ***Iceberg*** 글루 카탈로그가 업데이트됩니다.
        """
        st.markdown(elastic_text)
        st.markdown("""
            <hr style='margin-top: 10px; margin-bottom: 10px; border: 1px solid grey;'>
        """, unsafe_allow_html=True)

        # Iceberg and Elastic Search Columns
        iceberg1, iceberg2 = st.columns(2)

        # Iceberg Portion
        with iceberg1:
            how_to_text = """
            **Queryable Tables:** \n
            - sales
            - inventory
            - users
            - sales_aggregated 
            - inventory_aggregated \n
            :red[예시:] `SELECT * FROM sales LIMIT 10;` \n
            - 쿼리를 Pandas로 변형하여 표출하기에 칼렴 Alias 지정은 필요합니다
                - `SELECT COUNT(*) FROM sales;` :orange[(오류)]
                - `SELECT COUNT(*) AS count FROM sales;` :blue[(정상)]
            """
            st.markdown(how_to_text)
            query = st.text_input("SQL 쿼리 입력:")
            if st.button("Run Query"):
                with st.spinner("Running Query..."):
                    df = athena_query(query)  
                    st.session_state.iceberg_df = df

        with iceberg2:
            if st.session_state.iceberg_df is not None:
                st.markdown("### :violet[Iceberg Query Results]")
                st.dataframe(st.session_state.iceberg_df, use_container_width=True)
    
        job_id = None
        job_status = None
        st.markdown("""
            <hr style='margin-top: 10px; margin-bottom: 10px; border: 1px solid grey;'>
        """, unsafe_allow_html=True)
        st.markdown("#### :violet[Manually Trigger Batch Pipeline]")
        emr_text = """
        직접 배치 파이프라인을 트리거하여 ***EMR*** 클러스터를 생성하고 ***Spark*** 잡을 실행합니다. \n
        약 4-5분정도 소요되며 완료 후, \n
        위 모듈에서 SQL 쿼리를 통해 ***Iceberg Warehouse***가 업데이트 된 것을 확인하실 수 있습니다. \n
        :red[**많이 사용 하는 것은 자제 부탁드리겠습니다. (비용 발생)] \n
        """
        st.markdown(emr_text)
        # Trigger EMR job
        if st.button("Run EMR Job"):
            try:
                job_id = start_emr_job()
                st.success(f"EMR Job started with ID: {job_id}")
            except Exception as e:
                st.error(f"Failed to start EMR Job: {e}")

        if job_id:
            with st.spinner("Running EMR Job..."):
                while True:
                    job_status = monitor_emr_job(job_id)
                    if job_status != "RUNNING":
                        break
                    time.sleep(1)
            st.write(f"EMR Job Status: {job_status}")

    # Archi Tab
    with ArchiTab:
        image = Image.open('app/images/archi.png')
        st.image(image, caption="AWS Data Pipeline Architecture")
        st.markdown("### Source Code")
        st.markdown("GitHub: ")
        
if st.session_state.login_status:
    main_app()
    