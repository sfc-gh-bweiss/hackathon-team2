#pip install streamlit
#pip install snowflake-snowpark-python
#pip install "snowflake-snowpark-python[pandas]"
#pip install --upgrade streamlit
#pip install openpyxl
import streamlit as st
from snowflake.snowpark.session import Session
#from snowflake.snowpark.dataframe import DataFrame
import snowflake.snowpark.functions as F
import pandas as pd
import time
import pandas as pd
from PIL import Image


# home path to files (images)
HDIR="./"
session=None
def main():
	global HDIR
	session=None
	st.set_page_config(page_title="Team 2 Hackathon Consumer Accelerator")
	status=st.empty()

	#get session/connection values / set defaults
	qparams=st.experimental_get_query_params();
	if 'qparams' not in st.session_state:
		if 'account' in qparams.keys(): st.session_state.account=qparams.get('account')[0]
		else: st.session_state.account=""
		if 'user' in qparams.keys(): st.session_state.user=qparams.get('user')[0]
		else:  st.session_state.user=""
		if 'warehouse' in qparams.keys():st.session_state.warehouse=qparams.get('warehouse')[0]
		else: st.session_state.warehouse="DEMO_WH"
		if 'role' in qparams.keys():st.session_state.role=qparams.get('role')[0]
		else: st.session_state.role=""
		if 'database' in qparams.keys(): st.session_state.database=qparams.get('database')[0]
		else:  st.session_state.database="CLINICAL_SUBMISSIONS"
		if 'schema' in qparams.keys():st.session_state.schema=qparams.get('schema')[0]
		else: st.session_state.schema="RAW"
		if 'stage' in qparams.keys():st.session_state.stage=qparams.get('stage')[0]
		else: st.session_state.stage="Uploaded_Files"
		if 'table' in qparams.keys():st.session_state.table=qparams.get('table')[0]
		else: st.session_state.table=""
		st.session_state.contextSet=False
		st.session_state.connected=False
		st.session_state.qparams=True
	if 'session' in st.session_state.keys():	session = Session.builder.configs(st.session_state.session).create()

	tab1,tab2,tab3,tab4,tab5 = st.tabs(["Upload to Table", "Upload to Stage", "View Table","Download File","About Team#2"])

	with st.sidebar.expander("CONNECTION"):
		#account=st.text_input("account:",st.session_state.account)
		account=st.selectbox("account:",{'DEMO271','OTHER'})
		user=st.text_input("username:",st.session_state.user)
		password=st.text_input("password:",key="password",type="password")
		warehouse=st.text_input("warehouse:",st.session_state.warehouse)
		do_connect=st.button(label="Connect")
		if do_connect:
			try:
				if session is None:
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

	with st.sidebar.expander("CONTEXT",expanded=False):
		if st.session_state.connected:
			if st.session_state.contextSet:
				updateCtx=st.button(label="Update Context")
				if updateCtx:
					st.session_state.contextSet=False
			if not st.session_state.contextSet:
				roles = session.sql("show roles").select('"name"').to_pandas()
				role=st.selectbox("role:",pd.unique(roles['name']))

				databases = session.sql("show databases").select('"name"').filter((F.col('"name"') != 'SNOWFLAKE') & (F.col('"name"') != 'SNOWFLAKE_SAMPLE_DATA')).to_pandas()
				database=st.selectbox("database:",pd.unique(databases['name']))
				if(database):
					session.use_database(database)

				schemas = session.sql("show schemas").select('"name"').filter((F.col('"name"') != 'INFORMATION_SCHEMA')).to_pandas()
				#s = schemas[schemas['name'] == session.get_default_schema()].index
				#st.write(str(s)+"|||"+str(session.get_default_schema()))
				schema=st.selectbox("schema:",pd.unique(schemas['name']))
				session.use_schema(schema)

				stages = session.sql("show stages").select('"name"').to_pandas()
				stage=st.selectbox("stage:",pd.unique(stages['name']))

				do_context=st.button(label="Set Context")
				if do_context:
					st.session_state.role=role
					st.session_state.database=database
					st.session_state.schema=schema
					st.session_state.stage=stage
					try:
						st.session_state.contextSet=True
						st.session_state.session={'account':account,'user':user,'password':password,'warehouse':warehouse,'role':role,'database':database,'schemas':schema}
						showStatusMsg(status,"Context Set",True)
					except Exception as e:
						st.session_state.contextSet=False
						showStatusMsg(status,"Unable to Set Context: " + str(e),False)
		else:
			st.write("Create Connection First")

	st.sidebar.image(HDIR+"snowflakeLogo.jpg", use_column_width=True)

	with tab1:
		st.subheader("Upload File into Snowflake Table")
		file1 = st.file_uploader("Upload File",
			type=["xlsx","xls","csv"],key="file1",label_visibility="hidden")
		if file1 is not None:
			try:
				preview=st.checkbox("Preview",key='preview2table')
				df=None
				if file1.name.lower().endswith("xls") or file1.name.lower().endswith("xlsx"):
					df = pd.read_excel(file1)
				else:
					df=pd.read_csv(file1)
				if preview:
					st.markdown("""<hr style="height:10px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
					st.table(df)
					st.markdown("""<hr style="height:10px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

				if not st.session_state.connected:
					st.text_input(label="Table Name:",key="disabledTablename1",value="* Create Connection Before Continuing *",disabled=True)
				else:
					session.use_schema(st.session_state.schema)
					if not st.session_state.contextSet:
						st.text_input(label="Table Name:",key="disabledTablename2",value="* Set Context Before Continuing *",disabled=True)
					else:

						mapcolumns=st.checkbox("Tag Columns for Mapping",key='tagtable')
						if mapcolumns:
							st.markdown("""<hr style="height:10px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
							mapcol1, mapcol2 = st.columns(2)
							with mapcol1:
								st.write("Column1")

							with mapcol2:
								st.selectbox('TAG1','TAG2')
							st.markdown("""<hr style="height:10px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)


						tablemode = st.radio("Table Action:",('create', 'overwrite'))
						if tablemode=='create':
							tablename=st.text_input("Table Name:",st.session_state.table)
						else:
							tables = session.sql("show tables").select('"name"').to_pandas()
							tablename=st.selectbox("table:",pd.unique(tables['name']))
						if tablename:
							if st.button("Load to Table"):
									save2table = session.create_dataframe(df)
									save2table.write.mode("overwrite").save_as_table(tablename)
									msg="File Saved to Table: " + tablename
									if mapcolumns:
										associate_semantics(session, database+"."+schema+"."+tablename)
										msg+=" & Column Mappings Tags Applied"
									showStatusMsg(status,msg,True)
									st.snow()
			except Exception as e:
					showStatusMsg(status,"Unable to Save File to Table: " + str(e),False)

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
						st.session_state.stagename=stagename
						if st.button("Upload to Stage"):
							try:
								session.use_schema(st.session_state.schema)
								session.file.put(file2.getvalue(), "@"+stagename)
								showStatusMsg(status,msg,True)
							except Exception as e:
								showStatusMsg(status,"Unable to Save File to Stage: " + str(e),False)

	with tab3:
		st.subheader("Download Table from Snowflake")
		st.write("Under Construction")

	with tab4:
		st.subheader("Download File from Snowflake Stage")
		st.write("Under Construction")

	with tab5:
		st.subheader("Team #2")
		#Raj Kanniyappan
		st.write("Carter Faust, Michael Lazar, Steven Maser, Levi Pearce, Dirk Pluschke, JC Roboton, Irina Rubenchik, William Summerhill, Ben Weiss")
		st.write("2022 TKO2 Hackathon")
		st.write("Dallas, TX")
		st.balloons()

def associate_semantics(session,tblName):
	resp='Your column has been classified.'

	try:
		rows=session.sql("call associate_semantic_category_tags('"+tblName+"',extract_semantic_categories('"+tblName+"'));").collect()
		for r in rows:
			st.write(r)
	except Exception as err:
		resp=err
	return resp


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


