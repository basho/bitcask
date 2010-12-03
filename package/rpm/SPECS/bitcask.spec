# BuildArch should be determined automatically.  Use of setarch on x86-64
# platform will allow one to build ix86 only
#
# _revision, _release, and _version should be defined on the rpmbuild command
# line like so:
#
# --define "_version 0.9.1.19.abcdef" --define "_release 7" \
# --define "_revision 0.9.1-19-abcdef"


Name: bitcask
Version: %{_version}
Release: %{_release}%{?dist}
License: GPLv2
Group: Development/Libraries
Source: http://downloads.basho.com/%{name}/%{name}-%{_revision}/%{name}-%{_revision}.tar.gz
URL: http://basho.com/
Vendor: Basho Technologies
Packager: Basho Support <support@basho.com>
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Summary: Because you need another a key/value storage engine

%description
Because you need another a key/value storage engine

%define __prelink_undo_cmd /bin/cat prelink library

%prep
%setup -n %{name}-%{_revision}

%build
mkdir %{name}
# 2010-12-02: there are odd problems with get-deps in recent rebar releases.
# Bypassing the 'deps' target solves this problems when packaging from an
# archive tarball.
#ERL_FLAGS="-smp enable" make 
ERL_FLAGS="-smp enable" make

%install
mkdir -p %{buildroot}%{_libdir}%{name}

#Copy all necessary lib files etc.
cp -r $RPM_BUILD_DIR/%{name}-%{_revision}/ebin %{buildroot}%{_libdir}%{name}
cp -r $RPM_BUILD_DIR/%{name}-%{_revision}/priv %{buildroot}%{_libdir}%{name}
# I don't see how the source is useful at the moment
#cp -r $RPM_BUILD_DIR/%{name}-%{_revision}/src %{buildroot}%{_libdir}%{name}

%files
%defattr(-,root,root)
%dir %{_libdir}%{name}
%{_libdir}%{name}/*

%clean
rm -rf %{buildroot}

%changelog
* Wed May 26 2010 Ryan Tilder <rtilder@basho.com> 0.1-1
- Initial packaging

