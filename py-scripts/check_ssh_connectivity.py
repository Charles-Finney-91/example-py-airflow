import paramiko

def check_ssh_connectivity():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key_path = "c:/Users/charl/projects/example-py-airflow/ssh-keys/ssh_host_rsa_key"
        ssh.connect(
            hostname="localhost",
            port=2223,
            username="root",
            key_filename=private_key_path
        )
        print("SSH connection successful!")
    except paramiko.AuthenticationException:
        print("Authentication failed, please verify your credentials.")
    except paramiko.SSHException as ssh_exception:
        print(f"Unable to establish SSH connection: {ssh_exception}")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    check_ssh_connectivity()
