#pip install streamlit
#pip install snowflake-snowpark-python
#pip install "snowflake-snowpark-python[pandas]"
#pip install --upgrade streamlit
import streamlit as st
from snowflake.snowpark.session import Session
#from snowflake.snowpark.dataframe import DataFrame
import snowflake.snowpark.functions as F
import pandas as pd
import time
import numpy as np
import pandas as pd
import json
from PIL import Image


# home path to files (images)
HDIR="./"

def main():
	global HDIR
	session=None
	st.set_page_config(page_title="Team 2 Hackathon Consumer Accelerator")
	status=st.empty()
	menu = ["Upload","Download","About"]

	#get session/connection values / set defaults
	qparams=st.experimental_get_query_params();
	if 'qparams' not in st.session_state:
		if 'account' in qparams.keys(): st.session_state.account=qparams.get('account')[0]
		else: st.session_state.account=""
		if 'user' in qparams.keys(): st.session_state.user=qparams.get('user')[0]
		else:  st.session_state.user=""
		if 'warehouse' in qparams.keys():st.session_state.warehouse=qparams.get('warehouse')[0]
		else: st.session_state.warehouse=""
		if 'role' in qparams.keys():st.session_state.role=qparams.get('role')[0]
		else: st.session_state.role=""
		if 'database' in qparams.keys(): st.session_state.database=qparams.get('database')[0]
		else:  st.session_state.database=""
		if 'schema' in qparams.keys():st.session_state.schema=qparams.get('schema')[0]
		else: st.session_state.schema=""
		if 'stage' in qparams.keys():st.session_state.stage=qparams.get('stage')[0]
		else: st.session_state.stage=""
		if 'table' in qparams.keys():st.session_state.table=qparams.get('table')[0]
		else: st.session_state.table=""
		st.session_state.contextSet=False
		st.session_state.connected=False
		st.session_state.qparams=True
	if 'session' in st.session_state.keys():	session = Session.builder.configs(st.session_state.session).create()

	tab1,tab2,tab3,tab4,tab5 = st.tabs(["Upload to Table", "Upload to Stage", "Download Table","Download File","About"])

	with st.sidebar.expander("CONNECTION"):
		with st.form("connect_form"):
			account=st.text_input("account url:",st.session_state.account)
			user=st.text_input("username:",st.session_state.user)
			password=st.text_input("password:",key="password",type="password")
			warehouse=st.text_input("warehouse:",st.session_state.warehouse)
			do_connect=st.form_submit_button(label="Connect")
			if do_connect:
				try:
					session_parameters={'account':account,'user':user,'password':password,'warehouse':warehouse}
					session = Session.builder.configs(session_parameters).create()
					st.session_state.account=account
					st.session_state.user=user
					st.session_state.warehouse=warehouse
					st.session_state.role=session.get_current_role().replace('"','')
					st.session_state.connected=True
					st.session_state.session=session_parameters
					showStatusMsg(status,"Logged in Successfully as user ' "+session_parameters['user']+"'",True)
				except Exception as e:
					st.session_state.connected=False
					showStatusMsg(status,"Unable to Login, check your settings: " + str(e),False)

	with st.sidebar.expander("CONTEXT"):
		with st.form("context_form"):
			if st.session_state.connected:
				role=st.text_input("role:",st.session_state.role)

				databases = session.sql("show databases").select('"name"').filter((F.col('"name"') != 'SNOWFLAKE') & (F.col('"name"') != 'SNOWFLAKE_SAMPLE_DATA')).collect()
				st.write(databases)

				database=st.text_input("database:","hackathon")

				schemas = session.sql("show schemas").select('"name"').collect()
				st.write(schemas)

				schema=st.text_input("schema:","Public")

				stages = session.sql("show stages").select('"name"').collect()
				st.write(stages)

				stage=st.text_input("stage:","Uploaded Files")
				do_context=st.form_submit_button(label="Set Context")
				if do_context:
					st.session_state.role=role
					st.session_state.database=database
					st.session_state.schema=schema
					st.session_state.stage=stage
					st.session_state.contextSet=True
					showStatusMsg(status,"Context Set",True)
			else:
				st.write("Create Connection First")

	st.sidebar.image(HDIR+"snowflakeLogo.jpg", use_column_width=True)

	with tab1:
		st.subheader("Upload File into Snowflake Table")
		file1 = st.file_uploader("Upload File",
			type=["xlsx","xls","csv"],key="file1",label_visibility="hidden")
		if file1 is not None:
			preview=st.checkbox("Preview",key='preview2table')
			if preview:
				if file1.name.lower().endswith("xls") or file1.name.lower().endswith("xlsx"):
					df = pd.DataFrame(
						np.random.randn(10, 5),
						columns=('col %d' % i for i in range(5)))
					st.table(df)

				elif file1.name.lower().endswith("csv"):
					df=pd.read_csv(file1)
					st.dataframe(df)
				else:
					st.write("Preview Not Available")
			if not st.session_state.connected:
				st.text_input(label="Table Name:",key="disabledTablename1",value="* Create Connection Before Continuing *",disabled=True)
			else:
				if not st.session_state.contextSet:
					st.text_input(label="Table Name:",key="disabledTablename2",value="* Set Context Before Continuing *",disabled=True)
				else:
					tablename=st.text_input("Table Name:",st.session_state.table)
					if tablename:
						if st.button("Load to Table"):
							st.write("Loading File to Table....")

	with tab2:
		st.subheader("Upload File to Snowflake Stage")
		file2 = st.file_uploader("Upload File",label_visibility="hidden",key="file2",accept_multiple_files=False)
		if file2 is not None:
			preview2=st.checkbox("Preview",key='previewfile')
			if preview2:
				if file2.name.lower().endswith("png") or file2.name.lower().endswith("jpg"):
					st.image(load_image(file2),width=500)
				elif file2.name.lower().endswith("txt") or file2.name.lower().endswith("sql") or file2.name.lower().endswith("py"):
					st.write(str(file.read(),"utf-8"))
				else:
					st.write("Preview Not Available")
			if not st.session_state.connected:
				st.text_input(label="Stage Name:",key="disabledStagename1",value="* Create Connection Before Continuing *",disabled=True)
			else:
				if not st.session_state.contextSet:
						st.text_input(label="Stage Name:",key="disabledStagename2",value="* Set Context Before Continuing *",disabled=True)
				else:
					stagename=st.text_input("Stage Name:",st.session_state.stage)
					if stagename:
						if st.button("Upload to Stage"):
							st.write("Loading to Stage....")

	with tab3:
		st.subheader("Download Table from Snowflake")
		st.write("Under Construction")

	with tab4:
		st.subheader("Download File from Snowflake Stage")
		st.write("Under Construction")

	with tab5:
		st.subheader("About")
		st.write("Created by Snowflake")
		st.write("Team #2")
		st.write("2022 TKO2 Hackathon")

def load_image(file):
	img=Image.open(file)
	return img

def showStatusMsg(status,msg,isGood):
	if isGood == None:
		status.message(msg)
	elif isGood:
		status.success(msg)
	else:
		status.error(msg)
	time.sleep(3)
	status.empty()

if __name__ == "__main__":
	try:
		main()
	finally:
		#close all connections?
		connection = None
		if connection !=None:
			connection.close()


