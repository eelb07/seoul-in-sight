# Airflow Local 설치 가이드

> 참조: Airflow 공식 Docker 문서

## Airflow Docker 설치 단계

1. Docker Compose 파일 다운로드

    * Linux/Mac 사용자

        ```shell
        curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.1/docker-compose.yaml'
        ```

    * Windows 사용자 (PowerShell)

        ```powershell
        Invoke-WebRequest -Uri https://airflow.apache.org/docs/apache-airflow/3.0.1/docker-compose.yaml -OutFile 'docker-compose.yaml'
        ```

2. 환경 설정

    ```bash
    # 필요한 디렉토리 생성
    mkdir -p ./dags ./logs ./plugins ./config ./team1_dbt ./team1_dbt/.dbt

    # JWT 시크릿 키 생성 및 환경 변수 설정
    echo -e "AIRFLOW_UID=$(id -u)\nJWT_SECRET=$(openssl rand -hex 32)\n_PIP_ADDITIONAL_REQUIREMENTS=dbt-core==1.9.6 dbt-redshift==1.9.5\nDBT_UESR=changeme\nDBT_PASSWORD=changeme\nDBT_DATABASE=changeme" > .env
    ```

    > Windows

    ```powershell
    # 필요한 디렉토리 생성
    New-Item -ItemType Directory -Force -Path "./dags", "./logs", "./plugins", "./config", "./team1_dbt", "./team1_dbt/.dbt"

    # JWT 시크릿 키 생성 및 환경 변수 설정
    $AIRFLOW_UID = 1000
    $JWT_SECRET = -join ((48..57) + (65..70) | Get-Random -Count 32 | % { [char]$_ })
    "AIRFLOW_UID=1000`nJWT_SECRET=$JWT_SECRET`n_PIP_ADDITIONAL_REQUIREMENTS=dbt-core==1.9.6 dbt-redshift==1.9.5`nDBT_USER=changeme`nDBT_PASSWORD=changeme`nDBT_DATABASE=changeme" | Out-File -FilePath .env -Encoding utf8
    ```

    ```bash
    # docker-compose.yaml 파일 수정
    vim docker-compose.yaml
    ```

    `docker-compose.yml`에서 다음 설정을 확인/수정하세요

    ```yaml
    environment:
        &airflow-common-env
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'  # 예제 DAG 비활성화
        AIRFLOW__API_AUTH__JWT_SECRET: ${JWT_SECRET}  # JWT 인증을 위한 시크릿 키
    ```

3. Airflow 초기화

    ```bash
    docker compose up airflow-init
    ```

4. Airflow 서비스 시작

    ```bash
    docker compose up -d
    ```

5. Airflow 웹 인터페이스 접속

    서비스가 정상적으로 시작된 후, 웹 브라우저에서 [localhost:8080](http://localhost:8080)으로 접속하세요.

    * 기본 계정: `airflow`
    * 기본 비밀번호: `airflow`

6. DBT 관련
    DBT는 Airflow worker 내 설치되어 로컬 개발환경에서는 PostgreSQL을, 운영 환경에서는 Redshift를 대상으로 DBT를 실행합니다.

    생성된 `.env` 파일에 아래 항목을 수정합니다.

    - DBT_USER=`원하는 USER명`
    - DBT_PASSWORD=`원하는 Password`
    - DBT_DATABASE=`원하는 Database명`

    또한 `team1_dbt/.dbt/profiles.yml` 파일을 생성하셔서 (profiles.template.yml copy) db 접속 정보를 바꾸어 줍니다.

    - dbname: DBT_DATABASE값
    - user: DBT_USER값
    - pass: DBT_PASSWORD값

    운영환경에서는 Redshift를 사용할 예정이며, `target: prod` 설정으로 분기 처리 가능합니다. (추후 설정)

    DAG에서 `dbt run` 이외에도 `dbt test`, `dbt snapshot`, `dbt docs generate` 등의 연동이 가능합니다.
