from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from fpdf import FPDF
from docx import Document
import os

BASE_PATH = "/opt/airflow/files"

# -----------------------------
# Failure Callback Function
# -----------------------------
def send_failure_email(context):
    from airflow.utils.email import send_email

    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    html_content = f"""
        <h3>Hi Shivansh,</h3>
        <p>Your File Conversion DAG has <b style="color:red;">FAILED</b>.</p>
        <ul>
            <li><b>DAG:</b> {dag_id}</li>
            <li><b>Failed Task:</b> {task_id}</li>
            <li><b>Execution Date:</b> {execution_date}</li>
            <li><b>Error:</b> {exception}</li>
        </ul>
        <p>Please check the Airflow logs for more details.</p>
        <br>
        <p>Regards,<br>Airflow</p>
    """

    send_email(
        to='shivay999.ss@gmail.com',
        subject=f'❌ Airflow DAG Failed: {dag_id} | Task: {task_id}',
        html_content=html_content,
    )


# -----------------------------
# Default Args
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_failure_email,   # ← triggers on ANY task failure
}

dag = DAG(
    'failed_task_mail',
    default_args=default_args,
    description='Convert text file to Word and PDF and send email',
    schedule_interval=None,
    catchup=False,
)

# -----------------------------
# FileSensor - Wait for input.txt
# -----------------------------
def check_file_exists():
    return os.path.exists(f"{BASE_PATH}/input.txt")

wait_for_input_file = PythonSensor(
    task_id='wait_for_input_file',
    python_callable=check_file_exists,
    poke_interval=10,
    timeout=300,
    mode='reschedule',
    dag=dag,
)

# -----------------------------
# Read Text File
# -----------------------------
def read_text_file(ti):
    with open(f"{BASE_PATH}/input.txt", "r") as file:
        content = file.read()
    ti.xcom_push(key="file_content", value=content)


# -----------------------------
# Convert to DOCX
# -----------------------------
def convert_to_docx(ti):
    content = ti.xcom_pull(task_ids='read_text_file', key="file_content")
    doc = Document()
    doc.add_paragraph(content)
    output_path = f"{BASE_PATH}/output.docx"
    doc.save(output_path)
    ti.xcom_push(key="docx_path", value=output_path)


# -----------------------------
# Convert to PDF
# -----------------------------
def convert_to_pdf(ti):
    content = ti.xcom_pull(task_ids='read_text_file', key="file_content")
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    for line in content.split("\n"):
        pdf.cell(200, 10, txt=line, ln=True)
    output_path = f"{BASE_PATH}/output.pdf"
    pdf.output(output_path)
    ti.xcom_push(key="pdf_path", value=output_path)


# -----------------------------
# Define Tasks
# -----------------------------
create_text_task = PythonOperator(
    task_id='read_text_file',
    python_callable=read_text_file,
    dag=dag,
)

create_word_task = PythonOperator(
    task_id='convert_to_docx',
    python_callable=convert_to_docx,
    dag=dag,
)

create_pdf_task = PythonOperator(
    task_id='convert_to_pdf',
    python_callable=convert_to_pdf,
    dag=dag,
)

send_success_email = EmailOperator(
    task_id='send_success_email',
    to='shivay999.ss@gmail.com',
    subject='✅ Airflow File Conversion Successful 🎉',
    html_content="""
        <h3>Hi Shivansh,</h3>
        <p>Your File Conversion DAG has <b style="color:green;">completed successfully</b>.</p>
        <ul>
            <li><b>DAG:</b> {{ dag.dag_id }}</li>
            <li><b>Run ID:</b> {{ run_id }}</li>
            <li><b>Execution Date:</b> {{ ds }}</li>
        </ul>
        <p>PDF and DOCX files are attached.</p>
        <br>
        <p>Regards,<br>Airflow</p>
    """,
    files=[
        f"{BASE_PATH}/output.docx",
        f"{BASE_PATH}/output.pdf"
    ],
    conn_id='smtp_default',
    dag=dag,
)

# -----------------------------
# Task Flow
# -----------------------------
wait_for_input_file >> create_text_task >> [create_word_task, create_pdf_task] >> send_success_email
