%{!?upstream_version: %global upstream_version %{version}%{?milestone}}
%global pypi_name storage-gateway
%define _binaries_in_noarch_packages_terminate_build 0
%define _unpackaged_files_terminate_build 0

Name:		storage-gateway
Epoch:		1
Version:	v.1.0
Release:	1
Summary:	Storage-gatway

License:	ASL 2.0
URL:		https://github.com/Hybrid-Cloud/storage-gateway
#Source0:	%{name}-%{upstream_version}.tar.gz
Source0:	%{name}.tar.gz

#BuildRoot:	%_topdir/BUILDROOT
BuildRoot:	%{_tmppath}%{name}-%{version}-root
BuildArch:	noarch

Requires:	linux-vdso.so.1
Requires:	libboost_thread.so.1.54.0
Requires:	libgrpc++.so.1
Requires:	libboost_system.so.1.54.0
Requires:	libboost_log.so.1.54.0
Requires:	libpthread.so.0
Requires:	libstdc++.so.6
Requires:	libgcc_s.so.1
Requires:	libc.so.6
Requires:	librt.so.1
Requires:	libgrpc.so.3
Requires:	libboost_filesystem.so.1.54.0
Requires:	libm.so.6
Requires:	libs3.so.trunk0
Requires:	librados.so.2
Requires:	libdl.so.2
Requires:	libnss3.so
Requires:	libmime3.so
Requires:	libnssutil3.so
Requires:	libuuid.so.1
Requires:	libnspr4.so
Requires:	libplc4.so
Requires:	libplds4.so
Requires:	libz.so.1
Requires:	libsnappy.so.1
Requires:	libbz2.so.1.0

%description
Storage-gateway is a project that defines services for disaster recovery
between clouds.

%prep
%setup -n %{name}

%build
./build.sh
cd src/agent
make

%install
rm -rf %{buildroot}
rm -rf %{_rpmdir}/*
make install 
install -d -m 750 %{buildroot}%{_sysconfdir}/storage-gateway
install -d -m 755 %{buildroot}%{_bindir}/storage-gateway

install -p -m 755 etc/config.ini %{buildroot}%{_sysconfdir}/storage-gateway
install -p -D -m 755 bin/sg_server %{buildroot}%{_bindir}/storage-gateway
install -p -D -m 755 bin/sg_client %{buildroot}%{_bindir}/storage-gateway
install -p -D -m 755 bin/debug_client %{buildroot}%{_bindir}/storage-gateway
install -p -D -m 755 bin/journal_meta_utils %{buildroot}%{_bindir}/storage-gateway
install -p -D -m 755 src/agent/sg_agent.ko %{buildroot}%{_bindir}/storage-gateway

%clean
./build.sh clean
rm -rf $RPM_BUILD_ROOT

%files
#%defattr(-,root,root,-)
%config %attr(755, root, root) %{_sysconfdir}/storage-gateway/config.ini
%attr(755, root, root) %{_bindir}/storage-gateway/sg_server
%attr(755, root, root) %{_bindir}/storage-gateway/sg_client
%attr(755, root, root) %{_bindir}/storage-gateway/journal_meta_utils
%attr(755, root, root) %{_bindir}/storage-gateway/debug_client
%attr(755, root, root) %{_bindir}/storage-gateway/sg_agent.ko

%changelog

