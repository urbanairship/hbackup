Name:           hbackup
Summary:        %{name} UA Service
License:        All Rights Reserved
Version:        TEMPLATE
Release:        TEMPLATE
Source:         %{name}-%{version}.tar.gz
Group:          Development/Libraries
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root%(%{__Iid_u} -n)
BuildRequires:  maven
Requires:       jre rash


%description
Basic RPM for UA-style services


%define service_macro()               \
%package %1                           \
Summary:        %{name}-%1 package    \
AutoReqProv:    no                    \
Requires:       jre                   \
Group:          Development/Libraries \
%description %1                       \
%{name}-%1 UA Service                 \
%files %1                             \
%defattr(-,root,root)                 \
/mnt/services/%1/

%service_macro reports-backup
# The new mapr backup service
%service_macro hbackup-reports

%prep
%setup -q


%build
mvn -DskipTests install


%install
rm -rf %{buildroot}


%define install_macro() \
mkdir -p %{buildroot}/mnt/services/%1/rpm/lib/java \
cp target/*.jar %{buildroot}/mnt/services/%1/rpm/lib/java/ \
ln -s -T rpm %{buildroot}/mnt/services/%1/current

%install_macro reports-backup
%install_macro hbackup-reports

%clean
rm -rf $RPM_BUILD_ROOT
