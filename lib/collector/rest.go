package collector

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/gorilla"
)

func (collect *Collector) Scollector(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	points := gorilla.TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	returnPoints := RestErrors{}

	for _, point := range points {

		err := collect.HandlePacket(point, true)

		if err != nil {

			gblog.Sugar().Error(err.Error(), err.LogFields())

			ks := "default"
			if v, ok := point.Tags["ksid"]; ok {
				ks = v
			}

			statsPointsError(ks, "number")

			reu := RestErrorUser{
				Datapoint: point,
				Error:     err.Message(),
			}

			returnPoints.Errors = append(returnPoints.Errors, reu)

		} else {
			statsPoints(point.Tags["ksid"], "number")
		}
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) Text(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	points := gorilla.TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	returnPoints := RestErrors{}

	restChan := make(chan RestError, len(points))

	for _, point := range points {
		collect.concPoints <- struct{}{}
		go collect.handleRESTpacket(point, false, restChan)
	}

	var reqKS string
	var numKS int

	for range points {
		re := <-restChan
		if re.Gerr != nil {

			ks := "default"
			if v, ok := re.Datapoint.Tags["ksid"]; ok {
				ks = v
			}
			if ks != reqKS {
				reqKS = ks
				numKS++
			}

			statsPointsError(ks, "text")

			reu := RestErrorUser{
				Datapoint: re.Datapoint,
				Error:     re.Gerr.Message(),
			}

			returnPoints.Errors = append(returnPoints.Errors, reu)
		} else {
			pks := re.Datapoint.Tags["ksid"]
			if pks != reqKS {
				reqKS = pks
				numKS++
			}
			statsPoints(re.Datapoint.Tags["ksid"], "text")
		}
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) handleRESTpacket(rcvMsg gorilla.TSDBpoint, number bool, restChan chan RestError) {
	recvPoint := rcvMsg

	if number {
		rcvMsg.Text = ""
	} else {
		rcvMsg.Value = nil
	}

	restChan <- RestError{
		Datapoint: recvPoint,
		Gerr:      collect.HandlePacket(rcvMsg, number),
	}

	<-collect.concPoints
}
