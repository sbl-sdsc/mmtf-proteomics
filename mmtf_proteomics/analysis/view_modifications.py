def view_modifications(df, cutoff_distance, *args):

    def view3d(show_labels=True,show_bio_assembly=False, show_surface=False, i=0):
        pdb_id, chain_id = df.iloc[i]['structureChainId'].split('.')
        res_num = df.iloc[i]['pdbResNum']
        labels = df.iloc[i]['ptms']
        
        # print header
        print ("PDB Id: " + pdb_id + " chain Id: " + chain_id)
        
        # print any specified additional columns from the dataframe
        for a in args:
            print(a + ": " + df.iloc[i][a])
        
        mod_res = {'chain': chain_id, 'resi': res_num}  
        
        # select neigboring residues by distance
        surroundings = {'chain': chain_id, 'resi': res_num, 'byres': True, 'expand': cutoff_distance}
        
        viewer = py3Dmol.view(query='pdb:' + pdb_id, options={'doAssembly': show_bio_assembly})
    
        # polymer style
        viewer.setStyle({'cartoon': {'color': 'spectrum', 'width': 0.6, 'opacity':0.8}})
        # non-polymer style
        viewer.setStyle({'hetflag': True}, {'stick':{'radius': 0.3, 'singleBond': False}})
        
        # style for modifications
        viewer.addStyle(surroundings,{'stick':{'colorscheme':'orangeCarbon', 'radius': 0.15}})
        viewer.addStyle(mod_res, {'stick':{'colorscheme':'redCarbon', 'radius': 0.4}})
        viewer.addStyle(mod_res, {'sphere':{'colorscheme':'gray', 'opacity': 0.7}})
        
        # set residue labels    
        if show_labels:
            for residue, label in zip(res_num, labels):
                viewer.addLabel(residue + label, \
                                {'fontColor':'black', 'fontSize': 8, 'backgroundColor': 'lightgray'}, \
                                {'chain': chain_id, 'resi': residue})

        viewer.zoomTo(surroundings)
        
        if show_surface:
            viewer.addSurface(py3Dmol.SES,{'opacity':0.8,'color':'lightblue'})

        return viewer.show()
       
    s_widget = IntSlider(min=0, max=len(df)-1, description='Structure', continuous_update=False)
    
    return interact(view3d, show_labels=True,show_bio_assembly=False, show_surface=False, i=s_widget)
