FROM docker.pkg.github.com/paradicms/paradicms/paradicms:latest

ADD action.py /action.py

ENTRYPOINT ["/action.py"]
