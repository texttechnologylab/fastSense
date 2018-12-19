import mwparserfromhell as mwp


def parse_template(wikitext):
	try:
		template = mwp.parse(wikitext)
	except mwp.parser.ParserError:
		try:
			template = mwp.parse(template, skip_style_tags=True)
			print("[Warning] Skipped style tags in template!")
		except mwp.parser.ParserError:
			print("[Warning] Skipped template because of parse error!")
			return None

	template = template.get(0)
	if not isinstance(template, mwp.nodes.Template):
		return None

	return template


def Visible_anchor(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	params = template.params
	if len(params) == 0:
		return ""

	output_text = ""

	for i in range(1, len(params)):
		anchor_id = params[i].value.strip_code()
		anchor_id = anchor_id.replace("\"", "\\\"")
		output_text += "<span id=\"" + anchor_id + "\"></span>"

	output_text += params[0].value.strip_code()

	return output_text


def Anchor(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	params = template.params
	if len(params) == 0:
		return ""

	output_text = ""

	for i in range(len(params)):
		anchor_id = params[i].value.strip_code()
		anchor_id = anchor_id.replace("\"", "\\\"")
		output_text += "<span id=\"" + anchor_id + "\"></span>"

	return output_text


def Flag(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	if template.has_param("name"):
		return template.get("name").value.strip_code()
	elif template.has_param("1"):
		return template.get("1").value.strip_code()
	else:
		return ""


def As_of(template):
	return "As of DATE"


def Convert(template):
	return "MEASUREMENT"


def Nihongo(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	if len(template.params) == 0:
		return ""

	output_text = ""
	open_parentheses = False

	if template.has_param("1"):
		output_text += template.get("1").value.strip_code()

	if template.has_param("2"):
		output_text += " (" + template.get("2").value.strip_code() + "?"
		open_parentheses = True

	if template.has_param("3"):
		output_text += ", " + template.get("3").value.strip_code()

	if template.has_param("4"):
		output_text += ", " + template.get("4").value.strip_code()
	elif template.has_param("extra"):
		if open_parentheses:
			output_text += ", "
			output_text += template.get("extra").value.strip_code()

	if open_parentheses:
		output_text += ")"

	if template.has_param("5"):
		output_text += " " + template.get("5").value.strip_code()
	elif template.has_param("extra2"):
		output_text += " " + template.get("extra2").value.strip_code()

	return output_text


def Iast(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	if template.has_param("1"):
		return template.get("1").value.strip_code()
	else:
		return ""


def Quote(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	if len(template.params) == 0:
		return ""

	if template.has_param("text"):
		quote_text = template.get("text").value.strip_code()
	elif template.has_param("quote"):
		quote_text = template.get("quote").value.strip_code()
	elif template.has_param("quotetext"):
		quote_text = template.get("quotetext").value.strip_code()
	elif template.has_param("content"):
		quote_text = template.get("content").value.strip_code()
	elif template.has_param("1"):
		quote_text = template.get("1").value.strip_code()
	else:
		return ""

	return "<blockquote>" + quote_text + "</blockquote>"


def Rquote(template):
	template = parse_template(template.wikitext)
	if template is None:
		return ""

	if len(template.params) == 0:
		return ""

	if template.has_param("text"):
		quote_text = template.get("text").value.strip_code()
	elif template.has_param("quote"):
		quote_text = template.get("quote").value.strip_code()
	elif template.has_param("quotetext"):
		quote_text = template.get("quotetext").value.strip_code()
	elif template.has_param("content"):
		quote_text = template.get("content").value.strip_code()
	elif template.has_param("2"):
		quote_text = template.get("2").value.strip_code()
	else:
		return ""

	return "<blockquote>" + quote_text + "</blockquote>"


def table_start(template):
	return "\n\n{|"


def table_end(template):
	return "|}\n\n"


TEMPLATE_MAP = {
	"!": "&#124;",
	"!!": "&#124;&#124;",
	"=": "&#61;",
	"!(": "&#91;",
	")!": "&#93;",
	"!((": "&#91;&#91;",
	"))!": "&#93;&#93;",
	"(": "&#123;",
	")": "&#125;",
	"((": "&#123;&#123;",
	"))": "&#125;&#125;",
	"(((": "&#123;&#123;&#123;",
	")))": "&#125;&#125;&#125;",
	"Nbsp": "&nbsp;",
	"Visible_anchor": Visible_anchor,
	"Visanc": Visible_anchor,
	"Vanchor": Visible_anchor,
	"Anchor": Anchor,
	"Flag": Flag,
	"As_of": As_of,
	"Convert": Convert,
	"Nihongo": Nihongo,
	"Iast": Iast,
	"Quote": Quote,
	"Quote_box": Quote,
	"Cquote": Quote,
	"Quote_frame": Quote,
	"Rquote": Rquote,
	"Office-table": table_start,
	"S-start": table_start,
	"S-end": table_end,
	"Election_table": table_start,
	"Electiontable": table_start,
	"Award_table": table_start,
	"Awards_table": table_start,
	"End": table_end,
	"Static_column_begin": table_start,
	"Infobox_cricket_series_begin": table_start,
	"Infobox_cricket_series_end": table_end,
	"Col-begin": table_start,
	"Col_begin": table_start,
	"Col-start": table_start,
	"C-s": table_start,
	"Col-begin-small": table_start,
	"Col-end": table_end,
	"Col_end": table_end,
	"C-e": table_end,
	"Nobility_table_header": table_start,
	"Jctbtm": table_end,
	"NBA_player_statistics_start": table_start,
	"NBA_roster_statistics_start": table_start,
	"Euroleague_player_statistics_start": table_start,
	"Nat_fs_g_start": table_start,
	"Nat_fs_g_end": table_end,
	"Nat_fs_end": table_end,
	"Swimmingmedalisttabletop": table_start,
	"MMA_record_start": table_start,
	"TwoLeg_start": table_start,
	"TwoLegStart": table_start,
	"PBA_game_log_start": table_start,
	"PBA_game_log_end": table_end,
	"Game_log_start": table_start,
	"Game_log_end": table_end,
	"Rijksmonument_header": table_start,
	"AlumniimgStart": table_start,
	"AlumniEnd": table_end,
	"AlumniimgStartUK": table_start,
	"AlumniStart": table_start,
	"Member/start": table_start,
	"Mexico_TV_station_table/top": table_start,
	"Mexico_TV_station_table/bottom": table_end,
	"CanFlora-start": table_start,
	"Cabinet_table_start": table_start,
	"Cabinet_table_end": table_end,
	"Officeholder_table_start": table_start,
	"Officeholder_table_end": table_end,
	"BundleStart": table_start,
	"BundleEnd": table_end,
	"Boxing_record_start": table_start,
	"FacultyStart": table_start,
	"FacultyEnd": table_end,
	"HerolistStart": table_start,
	"HerolistEnd": table_end,
	"Listed_building_table_header": table_start,
	"Listed_building_table_footer": table_end,
	"Lists_of_United_Kingdom_locations_table_header": table_start,
	"StadiumimgStart": table_start,
	"StadiumimgEnd": table_end,
	"StadiumimgnorefStart": table_start,
	"EH_listed_building_header": table_start,
	"English_Heritage_listed_building_header": table_start,
	"Historic_building_header": table_start,
	"Election_FPTP_begin": table_start,
	"Election_Pref_begin": table_start,
	"Vb_res_start_2": table_start,
	"Vb_res_start_3": table_start,
	"Vb_res_start_4": table_start,
	"Vb_res_start_5": table_start,
	"Vb_res_start_6": table_start,
	"Vb_res_start_7": table_start,
	"Vb_res_start_8": table_start,
	"Vb_res_start_9": table_start,
	"Vb_res_start_10": table_start,
	"Vb_res_start_52": table_start,
	"Vb_cl_header": table_start,
	"Vb_cl2_header": table_start,
	"Vb_cl3_header": table_start,
	"Vb_cl4_header": table_start,
	"National_volleyball_squad_start": table_start,
	"National_volleyball_squad_end": table_end,
	"Fb_cl_header": table_start,
	"Fb_cl_footer": table_end,
	"Jcttop": table_start,
	"Chart_top": table_start,
	"Chart_bottom": table_end,
	"Election_summary_begin": table_start,
	"Election_Summary_Begin": table_start,
	"Election_Summary_Begin_with_Leaders": table_start,
	"Monarchs_-_table_header": table_start,
	"Pope_list_end": table_end,
	"Pope_list_begin": table_start,
	"CBB_schedule_start": table_start,
	"CBB_schedule_end": table_end,
	"CFB_Conference_Schedule_Start": table_start,
	"CFB_Conference_Schedule_End": table_end,
	"CFB_Schedule_Start": table_start,
	"CFB_Schedule_End": table_end,
	"CIH_schedule_start": table_start,
	"CIH_schedule_end": table_end,
	"CSOC_schedule_start": table_start,
	"CSOC_schedule_end": table_end,
	"MLB_Schedule_Start": table_start,
	"MLB_Schedule_End": table_end,
	"Basketball_player_statistics_start": table_start,
	"Basketball_player_statistics": table_start,
	"Basketball_box_score_header": table_start,
	"CBB_Standings_Start": table_start,
	"CBB_Standings_End": table_end,
	"Standings_Table_Start": table_start,
	"Standings_Table_End": table_end,
	"FIBA_player_statistics_start": table_start,
	"Hoops_team_start": table_start,
	"Hoops_team_footer": table_end,
	"Basketball_team_start": table_start,
	"Bt_start": table_start,
	"Basketball_team_end": table_end,
	"Bt_end": table_end,
	"HSBB_Yearly_Record_Start": table_start,
	"HSBB_Yearly_Record_End": table_end,
	"ThreeLegStart": table_start,
	"College_athlete_recruit_start": table_start,
	"College_athlete_recruit_end": table_end,
	"OneLegStart": table_start,
	"MinorPlanetNameMeaningsTableHeader": table_start,
	"List_of_minor_planets/header1": table_start,
	"List_of_minor_planets/header2": table_start,
	"Efs_start": table_start,
	"Efs_end": table_end,
	"Extended_football_squad_start": table_start,
	"Extended_football_squad_end": table_end,
	"Extended_football_squad_2_start": table_start,
	"Extended_football_squad_2_end": table_end,
	"Football_squad_start": table_start,
	"Fs_start": table_start,
	"Fs_end": table_end,
	"Football_squad_end": table_end,
	"Fs2_start": table_start,
	"Fs2_end": table_end,
	"National_football_squad_start": table_start,
	"National_football_squad_start_(goals)": table_start,
	"National_football_squad_start_(light)": table_start,
	"National_football_squad_start_(recent)": table_start,
	"National_football_squad_end": table_end,
	"Fb_cs_header": table_start,
	"Fb_cs_footer": table_end,
	"MNEinttop": table_start,
	"Jcttop/core": table_start,
	"Start_box": table_start,
	"End_box": table_end,
	"South_Korean_awards_table": table_start,
	"South_Korean_music_program_awards_table": table_start,
	"Awards_table2": table_start,
	"Awards_table3": table_start,
	"Awards_table4": table_start,
	"Start_NFL_RVO": table_start,
	"NFL_QB_stats_start": table_start,
	"NFL_Schedule_Start": table_start,
	"NFL_Schedule_End": table_end,
	"NFL_team_draft_start": table_start,
	"NFL_team_draft_end": table_end,
	"S-start-collapsible": table_start,
	"Canadian_election_result/top": table_start,
	"TLS-H": table_start,
	"TLS-H2": table_start,
	"Election_box_begin": table_start,
	"Election_box_begin_no_change": table_start,
	"CANelec/top": table_start,
	"Graphic_novel_list/header": table_start,
	"Graphic_novel_list/footer": table_end,
	"Nat_fs_start": table_start,
	"Election_box_end": table_end,
	"Election_box_2": table_start,
	"Canadian_politics/candlist_header": table_start,
	"MLB_Game_log_month": table_start,
	"MLB_Game_log_month_end": table_end,
	"Game_log_section_end": table_end,
	"MLB_Game_log": table_start,
	"MLB_game_log": table_start,
	"MIinttop": table_start,
	"J-railservice_start": table_start,
	"Scottish_Parliament_election": table_start,
	"Scottish_Constituency_Election_box_begin": table_start,
	"Scottish_Electoral_Area_Election_box_begin": table_start,
	"STV_Election_box_begin": table_start,
	"STV_Election_box_begin2": table_start,
	"STV_Election_box_begin3": table_start,
	"STV_Election_box_begin4": table_start,
	"STV_Election_box_begin5": table_start,
	"STV_Election_box_end": table_end,
	"STV_Election_box_end2": table_end,
	"STV_Election_box_end3": table_end,
	"STV_Election_box_end5": table_end,
	"Colort": table_start,
	"Certification_Table_Top": table_start,
	"Certification_Table_Bottom": table_end,
	"CanElec1-by": table_start,
	"CanElec4-by": table_start,
	"MunElec": table_start,
	"MunElec2": table_start,
	"MunElec4": table_start,
	"MunElec5": table_start,
	"MunElec6": table_start,
	"AFL_player_statistics_start": table_start,
	"LegSeats2": table_start,
	"LegSeats3": table_start,
	"BS-table": table_start,
	"Bs-table": table_start,
	"Ordinal_US_Congress_Senate": table_start,
	"Ordinal_US_Congress_Rep": table_start,
	"Election_city_polls_FPTP_begin": table_start,
	"Election_city_polls_FPTP_end": table_end,
	"BAB-exitlist": table_start,
	"Election_box_open_primary_begin_no_change": table_start,
	"Compact_election_box_begin": table_start,
	"Compact_election_box_no_change_begin": table_start,
	"Election_box_begin_for_list": table_start,
	"Election_box_begin_for_list_seats": table_start,
	"Election_box_begin_FPTP_multimember": table_start,
	"Election_box_begin_long_names": table_start,
	"Election_box_begin_no_clear": table_start,
	"Election_box_begin_no_party": table_start,
	"Election_box_begin_no_party_no_change": table_start,
	"Election_box_begin_no_party_no_change_with_delegates": table_start,
	"Election_box_begin_recount": table_start,
	"Election_box_begin-Nepal": table_start,
	"Election_box_inline_begin": table_start,
	"Election_box_inline_begin_no_change": table_start,
	"Election_box_ranked_choice_begin": table_start,
	"Election_box_referendums_begin": table_start,
	"Election_box_runningmate_begin": table_start,
	"Election_summary_begin_thirds": table_start,
	"Election_summary_begin_with_candidates": table_start,
	"Election_summary_net_begin": table_start,
	"Local_election_summary_begin": table_start,
	"Election_box_multidistrict_begin": table_start,
	"Compact_election_box_end": table_end,
	"Compact_election_box_no_change_end": table_end,
	"Election_box_inline_end": table_end,
	"Election_box_ranked_choice_end": table_end,
	"Fb_cl_header_navbar": table_start,
	"Fb_cl2_header_navbar": table_start,
	"Fb_cl2_header": table_start,
	"Fb_cl_header_H&A": table_start,
	"Fb_a_header": table_start,
	"Fb_cap_header": table_start,
	"Fb_cl3_header_navbar": table_start,
	"Fb_cm_header": table_start,
	"Fb_cs_ex_header": table_start,
	"Fb_disc_header": table_start,
	"Fb_disc2_header": table_start,
	"Fb_fm_header": table_start,
	"Fb_g_header": table_start,
	"Fb_in_header": table_start,
	"Fb_in2_header": table_start,
	"Fb_in2_mls_header": table_start,
	"Fb_kit_header": table_start,
	"Fb_lo_header": table_start,
	"Fb_loans_ended_header": table_start,
	"Fb_match_header": table_start,
	"Fb_match2_header": table_start,
	"Fb_mfs_header": table_start,
	"Fb_mls_header": table_start,
	"Fb_oi_header": table_start,
	"Fb_out_header": table_start,
	"Fb_out2_header": table_start,
	"Fb_out2_mls_header": table_start,
	"Fb_overall_competition_header": table_start,
	"Fb_ps_header": table_start,
	"Fb_r_header": table_start,
	"Fb_rbr_header": table_start,
	"Fb_rbr_header_Arsenal_12-13": table_start,
	"Fb_rbr_header_Eastern_Salon_13-14": table_start,
	"Fb_rbr_header_Esteghlal_F.C._11-12": table_start,
	"Fb_rbr_header_Kitchee_13-14": table_start,
	"Fb_rbr_header_la_liga_11-12": table_start,
	"Fb_rbr_header_São_Paulo_2013": table_start,
	"Fb_rbr_header_Southern_13-14": table_start,
	"Fb_rbr_header_Sun_Pegasus_13-14": table_start,
	"Fb_rbr_header_Yokohama_FC_HK_13-14": table_start,
	"Fb_rbr_pos_header": table_start,
	"Fb_rbr_pos_header2": table_start,
	"Fb_rbr_pos_header3": table_start,
	"Fb_sf_header": table_start,
	"Fb_sf_header_2": table_start,
	"Fb_si_header": table_start,
	"Fb_a_footer": table_end,
	"Fb_cl3_footer": table_end,
	"Fb_cm_footer": table_end,
	"Fb_cs_ex_footer": table_end,
	"Fb_disc_footer": table_end,
	"Fb_disc2_footer": table_end,
	"Fb_fm_footer": table_end,
	"Fb_g_footer": table_end,
	"Fb_in_footer": table_end,
	"Fb_kit_footer": table_end,
	"Fb_lo_footer": table_end,
	"Fb_match_footer": table_end,
	"Fb_mfs_footer": table_end,
	"Fb_MLS_club_footer": table_end,
	"Fb_MLS_club_footer2": table_end,
	"Fb_NASL_footer": table_end,
	"Fb_MLS_Western_Conference_club_footer": table_end,
	"Fb_nc_footer": table_end,
	"Fb_NPSL_table_footer": table_end,
	"Fb_NWSL_footer": table_end,
	"Fb_oi_footer": table_end,
	"Fb_out_footer": table_end,
	"Fb_out2_footer": table_end,
	"Fb_overall_competition_footer": table_end,
	"Fb_ps_footer": table_end,
	"Fb_r_footer": table_end,
	"Fb_rbr_footer": table_end,
	"Fb_rbr_footer_afl": table_end,
	"Fb_rbr_pos_footer": table_end,
	"Fb_rs_footer": table_end,
	"Fb_rs_mls_footer": table_end,
	"Fb_sf_footer": table_end,
	"Fb_si_footer": table_end,
	"Nat_fs_start_no_caps": table_start,
	"National_football_squad_start_(no_caps)": table_start,
	"Bs_cl2_header_navbar": table_start,
	"Bs_a_header": table_start,
	"Bs_cl_header": table_start,
	"Bs_cl3_header": table_start,
	"Bs_cl4_header": table_start,
	"Bs_game_header": table_start,
	"Bs_in2_header": table_start,
	"Bs_out2_header": table_start,
	"Bs_r_header": table_start,
	"Bs_r2_header": table_start,
	"Bs_rbr_header": table_start,
	"Bs_r_footer": table_end,
	"Bs_rbr_footer": table_end,
	"Bs_rs_footer": table_end,
	"Fb_rs": table_start,
	"NBA_game_log_start": table_start,
	"NCAASoftballScheduleStart": table_start,
	"NCAASoftballScheduleEnd": table_end,
	"Disused_Rail_Start": table_start,
	"Disused_rail_start": table_start,
	"Future_heritage_rail_start": table_start,
	"Future_rail_start": table_start,
	"Heritage_rail_start": table_start,
	"Historical_rail_start": table_start,
	"Historical_Rail_Start": table_start,
	"S-rail-start": table_start,
	"Service_rail_start": table_start,
	"WNBA_game_log_start": table_start,
	"WNBA_game_log_end": table_end,
	"College_ice_hockey_team_roster": table_start,
	"Ice_hockey_junior_team_roster": table_start,
	"Ice_hockey_minor_league_team_roster": table_start,
	"Ice_hockey_team_roster": table_start,
	"Junior_A_ice_hockey_team_roster": table_start,
	"Efs_start3": table_start,
	"Efs_start4": table_start,
	"Eredivisie_Playoff_ThreeLeg_start": table_start,
	"Football_season_start": table_start,
	"Football_squad_end2": table_end,
	"Football_squad_start2": table_start,
	"OneLeg_with_notes_start": table_start,
	"OneLeg_with_notes_end": table_end,
	"TwoLeg_with_notes_start": table_start,
	"TwoLeg_with_notes_end": table_end,
	"TwoLegStart_CONMEBOL": table_start,
	"TwoLegStart_CopaLib": table_start,
	"HK_Historic_Building_header": table_start,
	"WAinttop": table_start,
	"Start_srbox": table_start,
	"KSinttop": table_start,
	"CAinttop": table_start,
	"COinttop": table_start,
	"CTinttop": table_start,
	"ILinttop": table_start,
	"INinttop": table_start,
	"NHinttop": table_start,
	"OHinttop": table_start,
	"ORinttop": table_start,
	"PAinttop": table_start,
	"TXinttop": table_start,
	"VTinttop": table_start,
	"Irish_Election_box_begin": table_start,
	"Swimmingrecordlisttop": table_start,
	"NRHP_header": table_start,
	"Antarctic_Protected_Area_header": table_start,
	"ASI_Monument_header": table_start,
	"Botswana_Monument_header": table_start,
	"Cadw_listed_building_header": table_start,
	"Cardiff_listed_building_header": table_start,
	"CHL_header": table_start,
	"Ghana_Monument_header": table_start,
	"Ghana_Monument_header_WLE": table_start,
	"HK_Declared_Monument_header": table_start,
	"HPC_header": table_start,
	"HS_listed_building_header": table_start,
	"ISHM_header": table_start,
	"Kenya_Monument_header": table_start,
	"List_of_monuments_of_Algeria/header": table_start,
	"Manila_cultural_property_header": table_start,
	"National_Cultural_Site_of_Uganda_header": table_start,
	"Nepal_Monument_header": table_start,
	"NHS_China_header": table_start,
	"NHS_Japan_header": table_start,
	"NIEA_listed_building_header": table_start,
	"NRHP_former_header": table_start,
	"Philippine_cultural_property_header": table_start,
	"SAHRA_heritage_site_header": table_start,
	"SIoCPoNaRS_header": table_start,
	"Uganda_Monument_header": table_start,
	"UNESCO_World_Heritage_Site_header": table_start,
	"NYC_LPC_header": table_start,
	"HPC_footer": table_end,
	"List_of_monuments_of_Algeria/footer": table_end,
	"SIoCPoNaRS_footer": table_end,
	"Nsb_next_start": table_start
}
