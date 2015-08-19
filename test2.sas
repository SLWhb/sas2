%put _ALL_;


DATA _NULL_;
    put 1;
    put 2;
    aa=2;
    b=aa+4;
    if 1=2 then stop;
RUN;
