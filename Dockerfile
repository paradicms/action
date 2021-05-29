FROM python:3.9

RUN git clone --depth 1 https://github.com/paradicms/paradicms.git /paradicms &&
    cd paradicms/etl &&
    pip3 install . &&
    cd / &&
    rm -fr paradicms

ADD action.py /action.py

ENTRYPOINT ["/action.py"]
