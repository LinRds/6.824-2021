package raft

import "testing"

func Test_pack(t *testing.T) {
	type args struct {
		a uint32
		b uint32
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		{
			name: "packOneOne",
			args: args{
				a: 1,
				b: 1,
			},
			want: 0x0000000100000001,
		},
		{
			name: "pack2",
			args: args{
				a: 0x43752061,
				b: 0xFFFFFFFF,
			},
			want: 0x43752061FFFFFFFF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pack(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("pack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_unPack(t *testing.T) {
	type args struct {
		num uint64
	}
	tests := []struct {
		name  string
		args  args
		want  uint32
		want1 uint32
	}{
		{
			name:  "unPack1",
			args:  args{num: 0xFFFFFFFF12345678},
			want:  0xFFFFFFFF,
			want1: 0x12345678,
		},
		{
			name:  "unPack2",
			args:  args{num: 0xaaaabbbb34578700},
			want:  0xaaaabbbb,
			want1: 0x34578700,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := unPack(tt.args.num)
			if got != tt.want {
				t.Errorf("unPack() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("unPack() got1 = %x, want %x", got1, tt.want1)
			}
		})
	}
}
