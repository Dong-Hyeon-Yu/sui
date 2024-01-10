
```bash
pipx install --include-deps ansible
./boto3_install.yml
ansible-galaxy collection install -r requirements.yml      
ansible-inventory -i inventory/aws_ec2.yml --graph
ansible -m ping aws_ec2 -i inventory/aws_ec2.yml
./playbook/create_instances.yml
./playbook/install_dependencies.yml
./playbook/stop_instances.yml
./playbook/start_instances.yml
```