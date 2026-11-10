package main

import (
	"net/netip"
	"strings"
	"testing"
)

// 경로 종류 이름 <-> 번호
func TestFamilyNames(t *testing.T) {
	for _, s := range []string{"ipv4-unicast", "vpnv6", "l2vpn-evpn", "ipv4-flowspec", "bgp-ls"} {
		f, err := parseFamily(s)
		if err != nil || f.String() != s {
			t.Fatalf("%s : %v %v", s, f, err)
		}
	}
	if f, err := parseFamily("1/128"); err != nil || f.String() != "vpnv4" {
		t.Fatal(f, err)
	}
	if f, _ := parseFamily("99/9"); f.String() != "99/9" {
		t.Fatal(f)
	}
	if _, err := parseFamily("vpn"); err == nil {
		t.Fatal("unknown name must fail")
	}
}

// 해석기가 없는 종류 (BGP-LS MP_REACH) : 오류가 아니라 종류마다 세고 버림, 같은 UPDATE 의 unicast 는 그대로
func TestOtherFamily(t *testing.T) {
	// ORIGIN, AS_PATH 64496, MP_REACH bgp-ls (몸통은 해석하지 않음), NLRI 198.51.100.0/24
	mp := "4004" + "47" + "0c" + "0000000000000000" + "c0000201" + "00" + "70" + "000011" + "0000fbf000000001" + "cb0071"
	body := mustHex(t, "0000"+"0037"+"40010100"+"400206020100"+"00fbf0"+"400304c0000201"+"800e20"+mp+"18c63364")
	u, err := parseUpdate(body, pathOpts{as4: true})
	if err != nil || u.others != 1 || len(u.otherFams) != 1 || u.otherFams[0].String() != "bgp-ls" || len(u.v4Add) != 1 {
		t.Fatalf("%v others %d fams %v v4 %d", err, u.others, u.otherFams, len(u.v4Add))
	}
	// 그 종류의 End-of-RIB (빈 MP_UNREACH) 도 세고 버림 : unicast EoR 이 아님
	u, err = parseUpdate(mustHex(t, "0000"+"0006"+"800f03"+"400447"), pathOpts{as4: true})
	if err != nil || u.eorV4 || u.eorV6 || u.others != 1 {
		t.Fatalf("vpn EoR : %v %+v", err, u)
	}
	// 1 분 통계 글자
	var s stats
	s.addOther(famOf(afiIPv4, 128), 2)
	s.addOther(famOf(25, 70), 1)
	var total stats
	total.addAll(&s)
	if !strings.HasSuffix(total.String(), "other_families: vpnv4=2,l2vpn-evpn=1") {
		t.Fatal(total.String())
	}
}

// 두 OPEN 이 함께 알린 종류
func TestFamsOn(t *testing.T) {
	sent := openMsg{mp: map[uint32]bool{uint32(famOf(1, 1)): true, uint32(famOf(1, 128)): true, uint32(famOf(2, 1)): true}}
	recv := openMsg{mp: map[uint32]bool{uint32(famOf(1, 1)): true, uint32(famOf(1, 128)): true}}
	if got := strings.Join(familyList(famsOn(sent, recv)), ","); got != "ipv4-unicast,vpnv4" {
		t.Fatal(got)
	}
	if got := familyList(famsOn(openMsg{}, openMsg{})); len(got) != 1 || got[0] != "ipv4-unicast" { // MP 능력 없음 = IPv4 unicast
		t.Fatal(got)
	}
}

// -rib / bmp.rib : 종류 목록, 0.2 의 true / false 도
func TestRIBFamilies(t *testing.T) {
	for s, want := range map[string]string{
		"true": "ipv4-unicast,ipv6-unicast", "": "ipv4-unicast,ipv6-unicast", "false": "", "none": "",
		"all": "ipv4-unicast,ipv4-multicast,ipv4-labeled,vpnv4,ipv4-flowspec,vpnv4-flowspec,ipv6-unicast,ipv6-multicast,ipv6-labeled,vpnv6,ipv6-flowspec,vpnv6-flowspec", "ipv4-unicast, vpnv4": "ipv4-unicast,vpnv4", "ipv6-unicast": "ipv6-unicast",
	} {
		m, err := parseRIBFamilies(s)
		var fs []family
		for f := range m {
			fs = append(fs, f)
		}
		if got := strings.Join(familyList(fs), ","); err != nil || got != want {
			t.Errorf("%q : %q %v, want %q", s, got, err, want)
		}
	}
	if _, err := parseRIBFamilies("ipv4-unicast,vpn"); err == nil {
		t.Fatal("unknown family must fail")
	}
}

// IPv6 경로표만 끔 : IPv6 경로는 표에 없고 (Kafka 줄은 그대로), 옛 판이 넘긴 IPv6 표도 버림, API 경고
func TestRIBPartial(t *testing.T) {
	c := newCollector("x")
	fams, _ := parseRIBFamilies("ipv4-unicast")
	c.setRIB(fams)
	r := c.routerFor("192.0.2.1", "")
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64496})
	p.up = true
	one := func(s string) []prefix {
		x := netip.MustParsePrefix(s)
		return []prefix{{addr: x.Addr(), length: x.Bits()}}
	}
	u := &update{attrs: attrs{asPath: " 64496", nextHop: "192.0.2.1"}, v4Add: one("203.0.113.0/24"), v6Add: one("2001:db8::/32")}
	p.apply(viewInPre, u, testNow, c.on)
	if rv := p.view(-1); rv.rib4.size() != 1 || rv.rib6.size() != 0 || rv.adds != 2 {
		t.Fatalf("v4 %d v6 %d adds %d", rv.rib4.size(), rv.rib6.size(), rv.adds)
	}
	if j := c.peerView(r, p, false); !strings.Contains(strings.Join(j.Warn, ";"), "route table off for ipv6-unicast") {
		t.Fatal(j.Warn)
	}
	var tabs [nViews]*ribView
	tabs[viewInPre] = &ribView{}
	tabs[viewInPre].rib6.set(k6(netip.MustParseAddr("2001:db8::"), 32), route{})
	p.ribWait = true
	p.ribLoaded(tabs, c.on)
	if p.view(-1).rib6.size() != 0 {
		t.Fatal("ipv6 table from the old process must be dropped")
	}
}
