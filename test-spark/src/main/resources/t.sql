
(
    -- 新 image_id
    select
        old_im_tb.spu_id,
        old_im_tb.msn,
        old_im_tb.goods_id,
        im_map.new_image_id as image_id,
        old_im_tb.impression,
        old_im_tb.click,
        old_im_tb.cart
    from
        (
            ----------旧 image_id
            select
                k.spu_id,
                k.msn,
                k.goods_id,
                k.old_image_id,
                sum(k.impression) as impression,
                coalesce(sum(k.click),0) as click,
                coalesce(sum(k.cart),0) as cart
            from
                (
                    ------ 所有点击的商品 (spu_id,goods_id,msn)
                    select
                        sales.spu_id,
                        sales.msn,
                        old_ila.image_id as old_image_id,
                        old_ila.goods_id,
                        old_ila.impression,
                        old_ila.click,
                        0 as cart
                    from
                        (
                            ---------- 旧天表
                            select
                                goods_id,
                                image_id , --- 老的 image_id
                                impression ,
                                click
                            from
                                vipdws.dws_log_impression_intel_ui_imp_clk_stat_cd_df_mid
                            where dt='${dt}'
                        )old_ila
                            join
                        (
                            ----------- 关联 出 spu_id 与 msn
                            ----------销售表 全量表
                            select
                                merchandise_no as goods_id,
                                v_spu_id as spu_id,
                                m_sn as msn
                            from vipdw.dw_sales_merchandise
                            where dt= get_dt_date(sysdate(), -1) and is_deleted=0
                        )sales
                        on sales.goods_id=old_ila.goods_id
                )k
            group by k.spu_id,k.msn,k.goods_id,k.old_image_id
        )old_im_tb
            join
        ----- 映射表
            temp_vipreco.mtin_old_map_new_image_id_${dt} im_map
        on im_map.old_image_id=old_im_tb.old_image_id
)k


--====
join
(
SELECT
    spu_id,
    sn as msn,
    md5_value as image_id,
    min(size_id) as size_id
FROM vipdw.dw_vm_ias_m_list_image
WHERE dt=get_dt_date(sysdate(), -1)
group by spu_id,sn,md5_value

)