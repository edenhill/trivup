all:
	@echo "Targets:"
	@echo " bootstrap-ubuntu  - install required packages on Ubuntu/Debian (might need to be called thru sudo)"
	@echo " bootstrap-ubuntu-full - same as bootstrap-ubuntu but also includes docker (for Schema-Registry) and krb5-kdc (for Kerberos authentication)"


bootstrap-ubuntu-common:
	apt install -y curl openssl default-jre netcat
	which pip 2>/dev/null || apt install -y python-pip

bootstrap-ubuntu: bootstrap-ubuntu-common

bootstrap-ubuntu-full: bootstrap-ubuntu-common
	@echo "###### NOTICE: When asked to configure Kerberos realm, enter anything"
	@echo "###### NOTICE: and set the server to localhost. This configuration will"
	@echo "###### NOTICE: NOT be used by trivup and you should disable the krb5-kdc service by:"
	@echo "  sudo systemctl disable krb5-kdc ; sudo systemctl stop krb5-kdc"
	apt install -y krb5-kdc docker
