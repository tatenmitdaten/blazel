FROM public.ecr.aws/lambda/python:3.12 as builder

ENV ODBCINI=/opt/odbc.ini
ENV ODBCSYSINI=/opt/
ARG UNIXODBC_VERSION=2.3.9

RUN microdnf install -y gzip tar openssl-devel
RUN microdnf install -y \
    gcc \
    gcc-c++ \
    make \
    git \
    autoconf \
    automake \
    binutils \
    bison \
    flex \
    gettext \
    libtool \
    patch \
    pkgconfig

RUN curl ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-${UNIXODBC_VERSION}.tar.gz -O \
    && tar xzvf unixODBC-${UNIXODBC_VERSION}.tar.gz \
    && cd unixODBC-${UNIXODBC_VERSION} \
    && ./configure --sysconfdir=/opt --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --prefix=/opt \
    && make \
    && make install

# Add Microsoft repository
RUN curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo

# Install required packages
RUN dnf install -y \
    e2fsprogs \
    fuse-libs \
    libss

# Install ODBC Driver
RUN ACCEPT_EULA=Y dnf install -y msodbcsql17

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /opt/microsoft /opt/microsoft
COPY --from=builder /opt/lib /opt/lib