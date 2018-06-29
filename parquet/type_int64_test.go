package parquet

import (
	"testing"
)

func TestInt64PlainDecoder(t *testing.T) {
	testValuesDecoder(t, &int64PlainDecoder{}, []decoderTestCase{
		{
			data: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x9C, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
				0xEA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			decoded: []interface{}{
				int64(-9223372036854775808),
				int64(9223372036854775807),
				int64(0),
				int64(-100),
				int64(234),
			},
		},
	})
}

func TestInt64DeltaBianryPackedDecoder(t *testing.T) {
	testValuesDecoder(t, &int64DeltaBinaryPackedDecoder{}, []decoderTestCase{
		{
			data: []byte{
				0x80, 0x01, 0x08, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
				0xFF, 0xFF, 0xFF, 0x01, 0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
				0xFF, 0xFF, 0xFF, 0x01, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x9B, 0xFF,
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0x4D, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			decoded: []interface{}{
				int64(-9223372036854775808), int64(9223372036854775807),
				int64(0), int64(-100), int64(234),
			},
		},

		{
			data: []byte{
				0x80, 0x01, 0x08, 0x11, 0x0D, 0x02, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			decoded: []interface{}{
				int64(-7), int64(-6), int64(-5), int64(-4), int64(-3),
				int64(-2), int64(-1), int64(0), int64(1), int64(2), int64(3),
				int64(4), int64(5), int64(6), int64(7), int64(8), int64(9),
			},
		},

		{
			data: []byte{
				0x80, 0x01, 0x08, 0xC9, 0x01, 0xD0, 0x0F, 0x02, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00,
			},
			decoded: []interface{}{
				int64(1000), int64(1001), int64(1002), int64(1003), int64(1004),
				int64(1005), int64(1006), int64(1007), int64(1008), int64(1009),
				int64(1010), int64(1011), int64(1012), int64(1013), int64(1014),
				int64(1015), int64(1016), int64(1017), int64(1018), int64(1019),
				int64(1020), int64(1021), int64(1022), int64(1023), int64(1024),
				int64(1025), int64(1026), int64(1027), int64(1028), int64(1029),
				int64(1030), int64(1031), int64(1032), int64(1033), int64(1034),
				int64(1035), int64(1036), int64(1037), int64(1038), int64(1039),
				int64(1040), int64(1041), int64(1042), int64(1043), int64(1044),
				int64(1045), int64(1046), int64(1047), int64(1048), int64(1049),
				int64(1050), int64(1051), int64(1052), int64(1053), int64(1054),
				int64(1055), int64(1056), int64(1057), int64(1058), int64(1059),
				int64(1060), int64(1061), int64(1062), int64(1063), int64(1064),
				int64(1065), int64(1066), int64(1067), int64(1068), int64(1069),
				int64(1070), int64(1071), int64(1072), int64(1073), int64(1074),
				int64(1075), int64(1076), int64(1077), int64(1078), int64(1079),
				int64(1080), int64(1081), int64(1082), int64(1083), int64(1084),
				int64(1085), int64(1086), int64(1087), int64(1088), int64(1089),
				int64(1090), int64(1091), int64(1092), int64(1093), int64(1094),
				int64(1095), int64(1096), int64(1097), int64(1098), int64(1099),
				int64(1100), int64(1101), int64(1102), int64(1103), int64(1104),
				int64(1105), int64(1106), int64(1107), int64(1108), int64(1109),
				int64(1110), int64(1111), int64(1112), int64(1113), int64(1114),
				int64(1115), int64(1116), int64(1117), int64(1118), int64(1119),
				int64(1120), int64(1121), int64(1122), int64(1123), int64(1124),
				int64(1125), int64(1126), int64(1127), int64(1128), int64(1129),
				int64(1130), int64(1131), int64(1132), int64(1133), int64(1134),
				int64(1135), int64(1136), int64(1137), int64(1138), int64(1139),
				int64(1140), int64(1141), int64(1142), int64(1143), int64(1144),
				int64(1145), int64(1146), int64(1147), int64(1148), int64(1149),
				int64(1150), int64(1151), int64(1152), int64(1153), int64(1154),
				int64(1155), int64(1156), int64(1157), int64(1158), int64(1159),
				int64(1160), int64(1161), int64(1162), int64(1163), int64(1164),
				int64(1165), int64(1166), int64(1167), int64(1168), int64(1169),
				int64(1170), int64(1171), int64(1172), int64(1173), int64(1174),
				int64(1175), int64(1176), int64(1177), int64(1178), int64(1179),
				int64(1180), int64(1181), int64(1182), int64(1183), int64(1184),
				int64(1185), int64(1186), int64(1187), int64(1188), int64(1189),
				int64(1190), int64(1191), int64(1192), int64(1193), int64(1194),
				int64(1195), int64(1196), int64(1197), int64(1198), int64(1199),
				int64(1200),
			},
		},
	})
}
