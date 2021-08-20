FROM ghcr.io/paradicms/gui:latest

ADD action.py /action.py

ENTRYPOINT ["/action.py"]
