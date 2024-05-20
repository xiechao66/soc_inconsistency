package com.geely.general

object SOCFunc_NCM {

	/** NCM电池
	 * 根据公告型号，找出对应的单体电压-单体SOC插值表
	 *
	 * @param  declare_type 公告型号
	 * @return 返回单体电压-单体SOC插值表
	 */
	def searchOCV(declare_type: String): Array[Array[Double]] = {
		val ocv190CATL = Array(Array(3.382, 3.452, 3.47, 3.504, 3.549, 3.584, 3.613, 3.63, 3.646, 3.664, 3.687, 3.72, 3.774, 3.833, 3.888, 3.943, 4.001, 4.062, 4.123, 4.182, 4.239, 4.286, 4.317), Array(0.0, 4.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv180CATL = Array(Array(3.203, 3.35, 3.394, 3.452, 3.502, 3.548, 3.578, 3.603, 3.627, 3.654, 3.69, 3.745, 3.811, 3.859, 3.899, 3.94, 3.989, 4.043, 4.081, 4.106, 4.166), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv153LP = Array(Array(3.378, 3.469, 3.503, 3.548, 3.583, 3.613, 3.63, 3.646, 3.664, 3.687, 3.72, 3.774, 3.833, 3.888, 3.943, 4.001, 4.062, 4.123, 4.182, 4.239, 4.286, 4.317), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv153ZH = Array(Array(3.435, 3.468, 3.512, 3.552, 3.585, 3.612, 3.626, 3.641, 3.657, 3.677, 3.708, 3.766, 3.812, 3.861, 3.913, 3.966, 4.022, 4.082, 4.141, 4.202, 4.276), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv153RWD = Array(Array(3.415, 3.471, 3.520, 3.565, 3.596, 3.615, 3.629, 3.643, 3.659, 3.678, 3.707, 3.752, 3.799, 3.847, 3.898, 3.950, 4.005, 4.063, 4.124, 4.189, 4.277), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv153BE16 = Array(Array(3.353, 3.461, 3.503, 3.554, 3.592, 3.615, 3.630, 3.646, 3.664, 3.688, 3.721, 3.774, 3.837, 3.892, 3.947, 4.005, 4.065, 4.125, 4.183, 4.241, 4.275, 4.327), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv153G7 = Array(Array(3.393, 3.445, 3.458, 3.496, 3.535, 3.565, 3.596, 3.618, 3.634, 3.651, 3.672, 3.699, 3.739, 3.803, 3.859, 3.916, 3.975, 4.037, 4.101, 4.168, 4.238, 4.273, 4.326), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv153CATL = Array(Array(3.393, 3.430, 3.460, 3.500, 3.535, 3.565, 3.595, 3.620, 3.635, 3.655, 3.675, 3.710, 3.775, 3.837, 3.890, 3.945, 4.002, 4.063, 4.127, 4.195, 4.277), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv148CATL = Array(Array(3.44, 3.48, 3.52, 3.55, 3.59, 3.61, 3.62, 3.64, 3.65, 3.67, 3.71, 3.76, 3.81, 3.86, 3.91, 3.96, 4.02, 4.08, 4.14, 4.20, 4.28), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv128CATL = Array(Array(3.368, 3.432, 3.512, 3.538, 3.568, 3.595, 3.611, 3.626, 3.642, 3.663, 3.690, 3.733, 3.789, 3.841, 3.895, 3.949, 4.006, 4.067, 4.131, 4.200, 4.281), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv42CATL = Array(Array(3.440, 3.478, 3.516, 3.548, 3.577, 3.601, 3.615, 3.628, 3.641, 3.657, 3.680, 3.717, 3.756, 3.797, 3.841, 3.888, 3.938, 3.991, 4.047, 4.107, 4.177), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocvBP13 = Array(Array(3.338, 3.377, 3.430, 3.480, 3.527, 3.562, 3.587, 3.610, 3.633, 3.661, 3.701, 3.762, 3.813, 3.858, 3.898, 3.936, 3.983, 4.036, 4.076, 4.099, 4.154), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocvEX3 = Array(Array(3.476, 3.516, 3.553, 3.584, 3.608, 3.625, 3.639, 3.653, 3.669, 3.689, 3.72, 3.764, 3.804, 3.845, 3.888, 3.933, 3.98, 4.03, 4.081, 4.133, 4.191), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocvEX11 = Array(Array(3.338, 3.377, 3.430, 3.480, 3.527, 3.562, 3.587, 3.610, 3.633, 3.661, 3.701, 3.762, 3.813, 3.858, 3.898, 3.936, 3.983, 4.036, 4.076, 4.099, 4.154), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocvLG30 = Array(Array(3.335, 3.458, 3.553, 3.599, 3.627, 3.661, 3.734, 3.832, 3.925, 4.024, 4.139), Array(0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0))
		//新增
		val ocv173ZH = Array(Array(3.291, 3.443, 3.463, 3.493, 3.538, 3.574, 3.602, 3.629, 3.646, 3.663, 3.690, 3.716, 3.773, 3.829, 3.885, 3.940, 3.999, 4.057, 4.117, 4.176, 4.233, 4.260, 4.326), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv173RWD = Array(Array(3.4042, 3.4456, 3.4581, 3.4999, 3.5450, 3.5795, 3.6113, 3.6289, 3.6452, 3.6639, 3.6885, 3.7256, 3.7929, 3.8496, 3.9050, 3.9606, 4.0179, 4.0772, 4.1326, 4.1799, 4.2283, 4.2562, 4.3209), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv117 = Array(Array(3.287, 3.456, 3.495, 3.541, 3.574, 3.607, 3.625, 3.641, 3.658, 3.680, 3.712, 3.773, 3.835, 3.893, 3.954, 4.017, 4.081, 4.144, 4.203, 4.266, 4.305, 4.365), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv115 = Array(Array(3.247, 3.458, 3.495, 3.544, 3.581, 3.616, 3.636, 3.654, 3.674, 3.698, 3.733, 3.790, 3.857, 3.916, 3.977, 4.038, 4.100, 4.156, 4.203, 4.258, 4.353), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv115FC = Array(Array(3.434, 3.463, 3.470, 3.517, 3.557, 3.594, 3.620, 3.638, 3.655, 3.676, 3.702, 3.744, 3.805, 3.857, 3.909, 3.960, 4.013, 4.070, 4.124, 4.167, 4.204, 4.224, 4.281), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv51FC = Array(Array(3.365, 3.409, 3.456, 3.501, 3.541, 3.571, 3.591, 3.608, 3.626, 3.647, 3.680, 3.737, 3.788, 3.835, 3.881, 3.925, 3.973, 4.026, 4.075, 4.110, 4.180), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv51CATL = Array(Array(3.338, 3.377, 3.430, 3.480, 3.527, 3.562, 3.587, 3.610, 3.633, 3.661, 3.701, 3.762, 3.813, 3.858, 3.898, 3.936, 3.983, 4.036, 4.076, 4.099, 4.154), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv37CATL = Array(Array(3.397, 3.470, 3.513, 3.556, 3.589, 3.608, 3.621, 3.632, 3.645, 3.660, 3.679, 3.705, 3.752, 3.789, 3.829, 3.874, 3.923, 3.974, 4.028, 4.085, 4.180), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))

		//BX11
		val ocvBX11 = Array(Array(3.365, 3.409, 3.456, 3.501, 3.541, 3.571, 3.591, 3.608, 3.626, 3.647, 3.680, 3.737, 3.788, 3.835, 3.881, 3.925, 3.973, 4.026, 4.075, 4.110, 4.180), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))

		// DC1E
		val ocv248CAZE = Array(Array(3.345, 3.45, 3.49, 3.538, 3.574,	3.609, 3.627,	3.644, 3.662,	3.686, 3.722, 3.783, 3.85, 3.908,	3.967, 4.028, 4.089, 4.144, 4.188, 4.233, 4.315), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv248CATL = Array(Array(3.350, 3.448, 3.490, 3.537, 3.572, 3.607, 3.628, 3.645, 3.663, 3.686, 3.718, 3.774, 3.837, 3.896, 3.953, 4.010, 4.071, 4.127, 4.177, 4.227, 4.316), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))
		val ocv246XWD = Array(Array(3.433, 3.456, 3.471, 3.514, 3.556, 3.588, 3.619, 3.635, 3.650, 3.668, 3.690, 3.724, 3.788, 3.845, 3.899, 3.953, 4.010, 4.069, 4.126, 4.180, 4.235, 4.266, 4.317), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))
		val ocv_CATL_365Ah_2P108S = Array(Array(2.858, 3.085, 3.209, 3.343, 3.430, 3.467, 3.535, 3.594, 3.641, 3.679, 3.726, 3.770, 3.822, 3.894, 3.928, 3.977, 4.041, 4.061, 4.069, 4.085, 4.205), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))

		// EF1E
		val ocv_CATL_298Ah_2P108S = Array(Array(3.282, 3.454, 3.496, 3.545, 3.582, 3.614, 3.633, 3.650, 3.670, 3.698, 3.740, 3.812, 3.874, 3.933, 3.993, 4.055, 4.114, 4.164, 4.204, 4.257, 4.369), Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0))

		// BX1E
		val ocv_BE13_C_173Ah_1P107S = Array(Array(3.330,3.438,3.454,3.493,3.542,3.581,3.613,3.633,3.651,3.671,3.697,3.737,3.800,3.870,3.932,3.992,4.054,4.113,4.163,4.202,4.252,4.283,4.355), Array(0.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 100.0))

		// CS1E\DC1E-A2
		val ocv_CATL_150Ah_1P186S = Array(Array(3.102, 3.211, 3.265, 3.345, 3.434, 3.471, 3.494, 3.507, 3.548, 3.587, 3.621, 3.650, 3.672, 3.699, 3.738, 3.790, 3.864, 3.936, 3.997, 4.058, 4.117, 4.165, 4.203, 4.254, 4.286, 4.331, 4.352),
																														Array(0.0, 2.0, 3.0, 5.0, 8.0, 10.0, 13.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 97.0, 99.0, 100.0))



		val ocv = declare_type match {
			case "HQ7002BEV08" | "HQ7002BEV11" | "HQ7002BEV12" => ocv128CATL
			case "HQ7002BEV04" => ocv42CATL
			case "MR7002BEV03" => ocv42CATL
			case "HQ7002BEV05" | "HQ7002BEV35" | "HQ7002BEV36" | "HQ7002BEV56" | "HQ7002BEV56" | "HQ7002BEV37" | "HQ7002BEV39" | "MR7002BEV22" | "MR7002BEV23" | "JHC7002BEV38" | "JHC7002BEV39" | "HQ7002BEV15" => ocv153CATL
			case "JHC7002BEV27" | "JHC7002BEV51" => ocv180CATL
			case "JHC7002BEV34" | "HQ7002BEV45" | "HQ7002BEV57" | "JHC7002BEV33" | "JHC7002BEV34" | "HQ7002BEV57" | "HQ7002BEV66" => ocv148CATL
			case "JHC7003BEV02" | "JHC7003BEV06" | "HQ7003BEV04" | "HQ7002BEV68" | "JHC7002BEV48" | "HQ7002BEV84" => ocv190CATL
			case "HQ7003BEV01" | "JHC7003BEV01" | "HQ7002BEV70" | "HQ7002BEV67" | "JHC7002BEV47"  => ocv153G7
			case "JHC7002BEV45" | "HQ7002BEV72" | "JL7002BEV05" | "JHC7002BEV41" | "JHC7002BEV46" | "JHC7002BEV63" => ocv153LP
			case "JHC7002BEV25" | "HQ7002BEV51" => ocv153RWD
			case "JL7002BEV06" | "HQ7002BEV73" | "JHC7002BEV55" | "JHC7002BEV60" => ocv153RWD
			case "JHC7003BEV04" | "HQ7003BEV03" | "HQ7002BEV77" | "MR7002BEV28" | "HQ7002BEV76" | "HQ7002BEV78" | "HQ7003BEV05" => ocv153BE16
			case "HQ7002BEV63" | "JHC7002BEV26" | "HQ7002BEV63" => ocv153ZH
			case "JHC7003BEV03" | "JHC7002BEV64" => ocv153ZH
			case "MR6463DPHEV02" | "MR6453PHEV10" => ocvBP13
			case "HQ7004BEV01" => ocvEX3
			case "MR6501DPHEV01" => ocvEX11
			case "JL6453PHEV01" | "JL6453PHEV04" | "JL6453PHEV05" | "JL6453PHEV08" => ocvLG30
				//新增
			case "JL7000BEV02" | "JL7000BEV03" | "JL7000BEV04" | "JL7000BEV05" | "JL7000BEV06" | "JL7000BEV07" => ocv173ZH
			case "JL7000BEV08" | "JL7000BEV09" => ocv173RWD
			case "MR6501DCHEV01" => ocv117
			case "JL6482DCHEV03" => ocv115
			case "JL6482DCHEV02" => ocv115FC
			case "HQ7152DCHEV01" => ocv51FC
			case "HQ7152DCHEV02" => ocv51CATL
			case "MR7153PHEV08" | "MR7153PHEV05" | "MR7153PHEV18" | "MR7153PHEV01" =>ocv37CATL

				// BX11
			case "MR6432DPHEV03" | "MR6471DPHEV11" => ocvBX11
				// EF1E
			case "MR6525BEV06" => ocv_CATL_365Ah_2P108S
			case "MR6525BEV04" | "MR6525BEV05" => ocv_CATL_298Ah_2P108S

			// BX1E
			case "MR7005BEV05" | "MR7005BEV07" | "MR7005BEV09" => ocv_BE13_C_173Ah_1P107S
				// DC1E
			case "MR7001BEV03" | "MR7001BEV05" | "MR7001BEV08" | "MR7001BEV11" => ocv248CATL
			case "MR7001BEV04" | "MR7001BEV06" => ocv246XWD
			case "MR7001BEV15" | "MR7001BEV16" | "MR7001BEV17" | "MR7001BEV18" | "MR7001BEV20" | "MR7001BEV21" | "MR7001BEV36" | "MR7001BEV37"	=> ocv248CAZE
			case "MR7001BEV22" => ocv_CATL_365Ah_2P108S

				// DC1E-A2
			case "MR7001BEV27" => ocv_CATL_150Ah_1P186S

				// CS1E
			case "MR7005BEV12" | "MR7005BEV15" => ocv_CATL_150Ah_1P186S

			case _ => null
		}
		ocv
	}

}